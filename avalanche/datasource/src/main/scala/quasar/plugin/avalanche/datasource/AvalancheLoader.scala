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

import scala.Option

import doobie._

import fs2.Stream

import qdata.QType

import quasar.ScalarStages
import quasar.common.data.RValue
import quasar.connector.{DataFormat, QueryResult}
import quasar.connector.datasource.BatchLoader
import quasar.plugin.jdbc._
import quasar.plugin.jdbc.datasource._

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
  def apply(discovery: JdbcDiscovery, logHandler: LogHandler)
      : MaskedLoader[ConnectionIO, I] = {

    // 1. Need a type mapping function for (JdbcType, VendorTypeName) => Option[QType]
    //   - What do we do in the `None` case?
    //     - null for now?
    //   - Using option shouldn't really affect performance as we're only doing this once per result set
    //
    // 2. Write a function `((JdbcType, VendorType) => QType) => QDataDecode[ResultSet]`
    //
    // 3. (ResultSet, (JdbcType, VendorType) => QType) => Stream[F, RValue]

    // TODO: Update the ticket!!!!
    //
    // decided to implement QDataDecode and then just convert to RValue for now
    //
    // ipv4, ipv6 and UUID types will be represented as String, everything else
    // should be representable without a loss of fidelity
    BatchLoader.Full[ConnectionIO, (I, Option[I], ColumnSelection[I]), QueryResult[ConnectionIO]] {
      case (table, schema, columns) =>
        val dbObject0 =
          schema.fold(table.fr0)(_.fr0 ++ Fragment.const0(".") ++ table.fr0)

        val projections = columns match {
          case ColumnSelection.Explcit(idents) =>
            idents.map(_.fr0).intercalate(fr",")

          case ColumnSelection.All => fr"*"
        }

        val sql =
          fr"SELECT FOR READONLY" ++ projections ++ fr"FROM" ++ dbObject0

        // fetch size
        // read only
        // other cursor settings?

        FC.pure(QueryResult.typed[ConnectionIO](csvFormat, Stream.empty, ScalarStages.Id))
    }
  }

  def avalancheTypeToQType(jt: JdbcType, avalancheTypeName: String): Option[QType] =
    None

  def avalancheResultSetDecode: ResultSetIO[QDataDecode[ResultSet]]
    scala.Predef.???

  def rValueRead(decode: QDataDecode[ResultSet]): Read[RValue] =
    scala.Predef.???

  ////

}
