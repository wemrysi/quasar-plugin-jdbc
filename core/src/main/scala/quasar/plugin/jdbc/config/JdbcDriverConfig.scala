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

import java.net.URI

import moncle.macros.{GenPrism, Lenses}

sealed trait JdbcDriverConfig extends Product with Serializable

object JdbcDriverConfig {
  /** Configuration for a `javax.sql.DataSource`.
    *
    * @param className the fully-qualified name of the `javax.sql.DataSource` class provided by a JDBC driver.
    * @param properties used to configure the JDBC `DataSource`
    */
   @Lenses
  final case class JdbcDataSourceConfig(
      className: String,
      properties: Map[String, AnyRef])
      extends JdbcDriverConfig

  /** Configuration for a JDBC driver manged by `java.sql.DriverManager`.
    *
    * @param connectionUrl the database URL to use for new connections
    * @param driverClassName the fully-qualified class name of the JDBC driver. This
    *                        is usually automatically determined by the `connectionUrl`,
    *                        so only provide it if necessary.
    */
   @Lenses
  final case class JdbcDriverManagerConfig(
      connectionUrl: URI,
      driverClassName: Option[String])
      extends JdbcDriverConfig

  val jdbcDataSourceConfig: Prism[JdbcDriverConfig, JdbcDataSourceConfig] =
    GenPrism[JdbcDriverConfig, JdbcDataSourceConfig]

  val jdbcDriverManagerConfig: Prism[JdbcDriverConfig, JdbcDriverManagerConfig] =
    GenPrism[JdbcDriverConfig, JdbcDriverManagerConfig]
}
