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

import quasar.plugin.jdbc._
import quasar.plugin.jdbc.datasource._

import java.lang.{String, Throwable}

import cats.Defer
import cats.effect.Bracket

import doobie._
import doobie.implicits._

import quasar.connector.MonadResourceErr
import quasar.connector.datasource.LightweightDatasourceModule

import org.slf4s.Logger

// Initial impl will use cursors, might experiment with select loops later, though we'll lose backpressure
object AvalancheDatasource {
  // How to best select rows
  //
  // 1) COPY INTO would be ideal, but only allows files, not streaming
  //
  // 2) INSERT INTO EXTERNAL CSV would also be good, but again only allows files or hdfs
  //
  // 3) Quote string types, convert boolean to {0,1} and numeric, temporal types to strings
  //   + this should allow us to convert result sets to csv with reasonable performance
  //   - likely slower than 1) or 2) if either was supported

  def apply[F[_]](
      xa: Transactor[F],
      discovery: JdbcDiscovery,
      log: Logger,
      logHandler: LogHandler)
      : LightweightDatasourceModule.DS[F] = {

    // lookup columns for the table,
    // for string types, emit verbatim? we need to quote and escape quotes in
    // if it is temporal, use to_char with an appropriate format
    // otherwise use char
    //
    // build a column list of

    scala.Predef.???
  }
}
