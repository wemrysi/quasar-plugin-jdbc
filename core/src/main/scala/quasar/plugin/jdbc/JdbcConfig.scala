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

package quasar.plugin.jdbc

import java.net.URL

import scala.Int
import scala.concurrent.duration.FiniteDuration

trait JdbcConfig {
  /** The database connection URL without the `jdbc:` prefix. */
  def connectionUrl: URL

  /** The number of concurrent connections to allow to the database. */
  def maxConcurrentConnections: Int

  /** The duration to await validation of the initial connection to the database
    * when instantiating the plugin.
    */
  def connectionValidationTimeout: FiniteDuration
}
