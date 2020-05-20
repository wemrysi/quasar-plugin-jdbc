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

import quasar.ScalarStages
import quasar.connector.{DataFormat, QueryResult}
import quasar.connector.datasource.BatchLoader
import quasar.plugin.jdbc._
import quasar.plugin.jdbc.datasource._

object AvalancheLoader {
  // How to best select rows
  //
  // 1) COPY INTO would be ideal, but only allows files, not streaming
  //
  // 2) INSERT INTO EXTERNAL CSV would also be good, but again only allows files or hdfs
  //
  // 3) Quote string types, convert boolean to {0,1} and numeric, temporal types to strings
  //   + this should allow us to convert result sets to csv with reasonable performance
  //   - likely slower than 1) or 2) if either was supported
  def apply(
      discovery: JdbcDiscovery,
      hygiene: Hygiene,
      logHandler: LogHandler)
      : MaskedLoader[ConnectionIO, hygiene.HygienicIdent] = {

    type I = hygiene.HygienicIdent

    val csvFormat = DataFormat.SeparatedValues(
      header = false,
      row1 = '\r',
      row2 = '\n',
      record = ',',
      openQuote = '"',
      closeQuote = '"',
      escape = '"')

    // how do we handle escaping for CSV?
    //  A: seems like it would be better to just deal with this in scala, rather than try at the db.
    //
    // tuning fetch size seems like it will be a thing

    // GOAL: turn selected columns into strings, read them in chunks and render to csv
    //
    // find all columns, filter to just those selected
    // if the column is textual, leave it alone
    // if the column is numeric, leave it alone?
    // if the column is temporal, convert it to a string
    //   + this is easy on this side, as then all we have to do is handle the CSV encoding
    //   - will it be less efficient from a server pov? I guess unless it is select *, probably still has to seek and stuff to drop colums?
    //   - if it turns out to be less efficient, we can always move the temporal handling here

    BatchLoader.Full[ConnectionIO, (I, Option[I], ColumnSelection[I]), QueryResult[ConnectionIO]] {
      case (table, schema, columns) =>
        FC.pure(QueryResult.typed[ConnectionIO](csvFormat, Stream.empty, ScalarStages.Id))
    }
  }

  ////

}
