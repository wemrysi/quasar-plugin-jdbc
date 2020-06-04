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

package quasar.plugin.jdbc.config

import scala._, Predef._
import scala.concurrent.duration._
import scala.util.Either

import moncle.macros.Lenses

/** Configuration for a `doobie.Transactor`.
  *
  * @param driver describes how to configure and load the JDBC driver
  * @param connectionTimeout how long to await a database connection before erroring (default: 30s)
  * @param connectionValidationTimeout how long to await validation of a connection (default: 5s)
  * @param connectionMaxLifetime the maximum lifetime of a database connection,
  *                              idle connections exceeding their lifetime will be replaced with
  *                              new ones. Make sure to set this several seconds shorter than any
  *                              database or infrastructure imposed connection time limit. (default: 30min)
  * @param connectionMaxConcurrency the maximum number of concurrent connections to allow to the database
  */
@Lenses
final case class TransactorConfig(
    driver: Either[JdbcDriverManagerConfig, JdbcDataSourceConfig],
    connectionTimeout: FiniteDuration,
    connectionValidationTimeout: FiniteDuration,
    connectionMaxLifetime: FiniteDuration,
    connectionMaxConcurrency: Int)

object TransactorConfig {
  val DefaultConnectionTimeout: FiniteDuration = 30.seconds
  val DefaultConnectionValidationTimeout: FiniteDuration = 5.seconds
  val DefaultConnectionMaxLifetime: FiniteDuration = 30.minutes

  def withDefaultTimeouts(
      driver: Either[JdbcDriverManagerConfig, JdbcDataSourceConfig],
      connectionMaxConcurrency: Int)
      : TransactorConfig =
    TransactorConfig(
      driver,
      connectionTimeout = DefaultConnectionTimeout,
      connectionValidationTimeout = DefaultConnectionValidationTimeout,
      connectionMaxLifetime = DefaultConnectionMaxLifetime,
      connectionMaxConcurrency = connectionMaxConcurrency)
}
