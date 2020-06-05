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

import scala.{Int, Option, Some}
import scala.collection.immutable.SortedSet
import scala.concurrent.duration._
import scala.util.Either

import java.lang.String

import argonaut._, Argonaut._

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect._
import cats.implicits._

import doobie._

import eu.timepit.refined.auto._

import quasar.RateLimiting
import quasar.api.datasource.DatasourceType
import quasar.connector.{ByteStore, MonadResourceErr}
import quasar.connector.datasource.LightweightDatasourceModule
import quasar.plugin.jdbc.{JdbcDiscovery, TableType, TransactorConfig}
import quasar.plugin.jdbc.JdbcDriverConfig.JdbcDataSourceConfig
import quasar.plugin.jdbc.datasource.JdbcDatasourceModule

import org.slf4s.Logger

object AvalancheDatasourceModule extends JdbcDatasourceModule[DatasourceConfig] {

  val DefaultConnectionMaxConcurrency: Int = 8
  // Avalanche cloud appears to terminate idle connections after 4 minutes
  val DefaultConnectionMaxLifetime: FiniteDuration = 3.minutes + 30.seconds

  val kind = DatasourceType("avalanche", 1L)

  val DiscoverableTableTypes: Option[ConnectionIO[NonEmptySet[TableType]]] =
    Some(for {
      catalog <- HC.getCatalog
      rs <- HC.getMetaData(FDMD.getTableTypes)
      names <- FC.embed(rs, HRS.build[SortedSet, String])
      pruned = names.filterNot(_ == "SYSTEM TABLE")
      default = NonEmptySet.of("TABLE", "VIEW")
      discoverable = NonEmptySet.fromSet(pruned) getOrElse default
    } yield discoverable.map(TableType(_)))

  def transactorConfig(config: DatasourceConfig): Either[NonEmptyList[String], TransactorConfig] =
    config.connection.validated.toEither map { c =>
      val driverCfg =
        JdbcDataSourceConfig("com.ingres.jdbc.IngresDataSource", c.dataSourceProperites)

      val maxConcurrency =
        c.maxConcurrency getOrElse DefaultConnectionMaxConcurrency

      val maxLifetime =
        c.maxLifetime getOrElse DefaultConnectionMaxLifetime

      TransactorConfig
        .withDefaultTimeouts(
          driverConfig = driverCfg,
          connectionMaxConcurrency = maxConcurrency,
          connectionReadOnly = true)
        .copy(connectionMaxLifetime = maxLifetime)
    }

  def sanitizeConfig(config: Json): Json =
    config.as[DatasourceConfig].toOption
      .fold(jEmptyObject)(_.sanitized.asJson)

  def jdbcDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer, A](
      config: DatasourceConfig,
      transactor: Transactor[F],
      rateLimiter: RateLimiting[F, A],
      byteStore: ByteStore[F],
      log: Logger)
      : Resource[F, Either[InitError, LightweightDatasourceModule.DS[F]]] = {

    val discovery = JdbcDiscovery(DiscoverableTableTypes)

    AvalancheDatasource(transactor, discovery, log)
      .asRight[InitError]
      .pure[Resource[F, ?]]
  }
}
