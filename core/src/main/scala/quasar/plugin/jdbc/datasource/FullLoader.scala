/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.plugin.jdbc.datasource

import quasar.plugin.jdbc._
import quasar.plugin.jdbc.implicits._

import scala.{None, Option, Some}
import scala.util.{Left, Right}

import cats.{Defer, Monad}
import cats.effect.Resource
import cats.implicits._

import doobie._

import quasar.ScalarStages
import quasar.api.resource.ResourcePath
import quasar.connector.{MonadResourceErr, QueryResult, ResourceError => RE}
import quasar.connector.datasource._
import quasar.qscript.InterpretedRead

object FullLoader {
  /** A `BatchLoader` that loads the entire contents of a table.
    *
    * @param xa the transactor to execute sql statements with
    * @param discovery used to determine whether a query path refers to a table
    * @param hygiene a means of obtaining hygienic identifiers
    * @param load returns the `QueryResult` for the given table, schema and scalar stages
    */
  def apply[F[_]: Defer: Monad: MonadResourceErr](
      xa: Transactor[F],
      discovery: JdbcDiscovery,
      hygiene: Hygiene)(
      load: (hygiene.HygienicIdent, Option[hygiene.HygienicIdent], ScalarStages) => ConnectionIO[QueryResult[ConnectionIO]])
      : BatchLoader[Resource[F, ?], InterpretedRead[ResourcePath], QueryResult[F]] =
    BatchLoader.Full { (ir: InterpretedRead[ResourcePath]) =>
      resourcePathRef(ir.path) match {
        case Some(ref) =>
          val (table, schema) = ref match {
            case Left(table) => (table, None)
            case Right((schema, table)) => (table, Some(schema))
          }

          val result = discovery.tableExists(table, schema) flatMap { exists =>
            if (exists)
              load(hygiene.hygienicIdent(table), schema.map(hygiene.hygienicIdent(_)), ir.stages)
                .map(_.asRight[RE])
            else
              FC.pure(RE.pathNotFound[RE](ir.path).asLeft[QueryResult[ConnectionIO]])
          }

          xa.strategicConnection evalMap { c =>
            xa.runWith(c).apply(result) flatMap {
              case Left(re) =>
                MonadResourceErr[F].raiseError[QueryResult[F]](re)

              case Right(qr) =>
                qr.mapK(xa.runWith(c)).pure[F]
            }
          }

        case None =>
          Resource.liftF(MonadResourceErr[F].raiseError(RE.notAResource(ir.path)))
      }
    }
}
