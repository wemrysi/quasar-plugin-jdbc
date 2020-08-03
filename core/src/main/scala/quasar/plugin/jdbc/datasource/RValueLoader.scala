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

import scala.{Stream => _, _}

import java.sql.ResultSet

import cats.effect.Resource
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.Stream

import quasar.ScalarStages
import quasar.common.data.{QDataRValue, RValue}
import quasar.connector.QueryResult
import quasar.connector.datasource.BatchLoader

object RValueLoader {
  type Args[A] = (A, Option[A], ColumnSelection[A], ScalarStages)

  def apply[I <: Hygienic](
      logHandler: LogHandler,
      resultChunkSize: Int,
      rvalueColumn: RValueColumn)
      : BatchLoader[Resource[ConnectionIO, ?], Args[I], QueryResult[ConnectionIO]] =
    BatchLoader.Full[Resource[ConnectionIO, ?], Args[I], QueryResult[ConnectionIO]] {
      case (table, schema, columns, stages) =>
        val dbObject0 =
          schema.fold(table.fr0)(_.fr0 ++ fr0"." ++ table.fr0)

        val projections = Some(columns) collect {
          case ColumnSelection.Explicit(idents) =>
            idents.map(_.fr0).intercalate(fr",")

          case ColumnSelection.All => fr0"*"
        }

        val rvalues = projections match {
          case Some(prjs) =>
            val sql =
              (fr"SELECT" ++ prjs ++ fr" FROM" ++ dbObject0).query[Unit].sql

            val ps =
              FC.prepareStatement(
                sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY)

            loggedRValueQuery(sql, ps, resultChunkSize, logHandler)(
              rvalueColumn.isSupported,
              rvalueColumn.unsafeRValue)

          case None =>
            (Stream.empty: Stream[ConnectionIO, RValue]).pure[Resource[ConnectionIO, ?]]
        }

        rvalues.map(QueryResult.parsed(QDataRValue, _, stages))
    }
}
