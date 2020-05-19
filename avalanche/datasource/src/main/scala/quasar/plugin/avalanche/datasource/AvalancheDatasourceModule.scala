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

import scala.util.Either

import argonaut._, Argonaut._

import cats.Hash
import cats.effect._
import cats.implicits._

import doobie._

import eu.timepit.refined.auto._

import io.chrisdavenport.log4cats.Logger

import quasar.RateLimiting
import quasar.api.datasource.DatasourceType
import quasar.connector.{ByteStore, MonadResourceErr}
import quasar.connector.datasource.LightweightDatasourceModule
import quasar.plugin.jdbc.datasource.JdbcDatasourceModule

object AvalancheDatasourceModule extends JdbcDatasourceModule[Config]("org.some.avalanche.Driver") {
  val kind = DatasourceType("avalanche", 1L)

  def sanitizeConfig(config: Json): Json =
    config.as[Config].toOption
      .fold(jEmptyObject)(_.sanitized.asJson)

  def jdbcDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer, A](
      config: Config,
      transactor: Transactor[F],
      rateLimiter: RateLimiting[F, A],
      byteStore: ByteStore[F],
      log: Logger[F],
      logHandler: LogHandler)
      : Resource[F, Either[InitError, LightweightDatasourceModule.DS[F]]] =
    scala.Predef.???
}
