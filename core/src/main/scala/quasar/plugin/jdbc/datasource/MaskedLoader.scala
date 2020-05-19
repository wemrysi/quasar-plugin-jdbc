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

import scala.Option

import cats.{Defer, Monad}
import cats.effect.Resource

import doobie._

import quasar.api.resource.ResourcePath
import quasar.connector.{MonadResourceErr, QueryResult}
import quasar.connector.datasource._
import quasar.qscript.InterpretedRead

object MaskedLoader {
  /** A `BatchLoader` that loads the entire contents of a table, interpreting an
    * initial `Mask` stage as column selection.
    *
    * @param xa the transactor to execute sql statements with
    * @param discovery used to determine whether a query path refers to a table
    * @param hygiene a means of obtaining hygienic identifiers
    * @param load returns the `QueryResult` for the given table, schema and column selection
    */
  def apply[F[_]: Defer: Monad: MonadResourceErr](
      xa: Transactor[F],
      discovery: JdbcDiscovery,
      hygiene: Hygiene)(
      load: (hygiene.HygienicIdent, Option[hygiene.HygienicIdent], ColumnSelection[hygiene.HygienicIdent]) => ConnectionIO[QueryResult[ConnectionIO]])
      : BatchLoader[Resource[F, ?], InterpretedRead[ResourcePath], QueryResult[F]] =
    FullLoader(xa, discovery, hygiene) { (table, schema, stages) =>
      MaskedColumns(stages).fold(load(table, schema, ColumnSelection.All)) {
        case (cols, moreStages) =>
          val explicit = ColumnSelection.Explicit(cols.map(hygiene.hygienicIdent(_)))
          load(table, schema, explicit)
      }
    }
}
