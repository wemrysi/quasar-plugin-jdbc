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

package quasar.plugin.avalanche.datasource

import slamdata.Predef._

import quasar.plugin.avalanche._

import java.sql.ResultSet

import cats.effect.Resource
import cats.implicits._

import doobie._
import doobie.enum.JdbcType
import doobie.implicits._

import fs2.Stream

import qdata.{QData, QDataDecode, QType}

import quasar.ScalarStages
import quasar.common.data.RValue
import quasar.connector.QueryResult
import quasar.connector.datasource.BatchLoader
import quasar.plugin.jdbc._
import quasar.plugin.jdbc.datasource._

// TODO: Logging
object AvalancheLoader {
  type I = AvalancheHygiene.HygienicIdent

  // How to best select rows
  //
  // 1) COPY INTO would be ideal, but only allows files, not streaming
  //
  // 2) INSERT INTO EXTERNAL CSV would also be good, but again only allows files or hdfs
  //
  // 3) Quote string types, convert boolean to {0,1} and numeric, temporal types to strings
  //   + this should allow us to convert result sets to csv with reasonable performance
  //   - likely slower than 1) or 2) if either was supported
  def apply(discovery: JdbcDiscovery, resultChunkSize: Int)
      : MaskedLoader[ConnectionIO, I] = {

    // 1. Need a type mapping function for (JdbcType, VendorTypeName) => Option[QType]
    //   - What do we do in the `None` case?
    //     - null for now?
    //   - Using option shouldn't really affect performance as we're only doing this once per result set
    //
    // 2. Write a function `((JdbcType, VendorType) => QType) => QDataDecode[ResultSet]`
    //
    // 3. (ResultSet, (JdbcType, VendorType) => QType) => Stream[F, RValue]

    // decided to implement QDataDecode and then just convert to RValue for now
    //
    // ipv4, ipv6 and UUID types will be represented as String, everything else
    // should be representable without a loss of fidelity
    BatchLoader.Full[Resource[ConnectionIO, ?], (I, Option[I], ColumnSelection[I]), QueryResult[ConnectionIO]] {
      case (table, schema, columns) =>
        val dbObject0 =
          schema.fold(table.fr0)(_.fr0 ++ Fragment.const0(".") ++ table.fr0)

        val projections = columns match {
          case ColumnSelection.Explicit(idents) =>
            idents.map(_.fr0).intercalate(fr",")

          case ColumnSelection.All => fr"*"
        }

        val query =
          fr"SELECT" ++ projections ++ fr"FROM" ++ dbObject0

        val preparedStatement =
          Resource.make(
            FC.prepareStatement(
              query.query[Unit].sql,
              ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY))(
            FC.embed(_, FPS.close))

				// 1. Prepare statement
				// 2. Set forward only
        // 3. Build QDataDecode from result set metadata
        // 4. Execute query, building decoder, converting to Read and then streaming via result set means?




        // fetch size [will set based on chunk size]
        // read only [done in query]
        // other cursor settings [need to set forward only]

        // do we need the result set to build the decoder? or can we write one for any result set?
        //sql.query(rValueRead(avalancheResultSetDecode)

        scala.Predef.???
    }
  }
}
