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

import doobie._

import quasar.ScalarStages
import quasar.connector.QueryResult
import quasar.connector.datasource._

// TODO: Doobie query logging
trait FullLoading[F[_]] { self: JdbcDatasource[F] with Hygiene =>

  def tableResult(table: TableName, schema: Option[SchemaName], scalarStages: ScalarStages)
      : QueryResult[ConnectionIO]

  ////

  val loaders = NonEmptyList.of(Loader.Batch(BatchLoader.Full { (ir: InterpretedRead[ResourcePath]) =>
    Resource.liftF(pathIsResource(ir.path)) map { isResource =>
      resourcePathRef(ir.path) match {
        case Some(Right((schema, table))) =>
          val back = tableExists(schema, table) map { exists =>
            if (exists)
              Right(maskedColumns(ir.stages) match {
                case Some((columns, nextStages)) =>
                  (tableAsJsonBytes(schema, ColumnProjections.Explicit(columns), table), nextStages)

                case None =>
                  (tableAsJsonBytes(schema, ColumnProjections.All, table), ir.stages)
              })
            else
              Left(RE.pathNotFound[RE](ir.path))
          }

          xa.connect(xa.kernel)
            .evalMap(c => runCIO(c)(back.map(_.map(_.leftMap(_.translate(runCIO(c)))))))
            .evalMap {
              case Right((s, stages)) =>
                QueryResult.typed(DataFormat.ldjson, s, stages).pure[F]

              case Left(re) =>
                MonadResourceErr[F].raiseError[QueryResult[F]](re)
            }

        case _ =>
          Resource.liftF(MonadResourceErr[F].raiseError(RE.notAResource(ir.path)))
      }
    }
  }))
}
