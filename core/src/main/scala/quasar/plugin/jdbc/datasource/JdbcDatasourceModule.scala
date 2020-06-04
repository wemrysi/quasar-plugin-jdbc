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

import quasar.plugin.jdbc.Redacted
import quasar.plugin.jdbc.config._

import java.lang.{Exception, RuntimeException, String}
import java.net.URI
import java.util.concurrent.Executors

import scala.{Int, StringContext, Unit}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{Either, Left, Random, Right}
import scala.util.control.NonFatal

import argonaut._, Argonaut._

import cats.Hash
import cats.data.{EitherT, NonEmptyList}
import cats.effect._
import cats.implicits._

import doobie._
import doobie.hikari.HikariTransactor
import doobie.implicits._

import quasar.{concurrent => qc, RateLimiting}
import quasar.api.datasource.{DatasourceError => DE}
import quasar.connector.{ByteStore, MonadResourceErr}
import quasar.connector.datasource.LightweightDatasourceModule

import org.slf4s.{Logger, LoggerFactory}

/** A Quasar LightweightDatsourceModule for JDBC sources.
  *
  * Handles boilerplate common to all JDBC datasources, such as
  *   - parsing JSON into a vendor-specific config
  *   - constructing a pooled `Transactor` along with the necessary threadpools
  *   - validating a connection to the database can be established
  *   - logging
  */
abstract class JdbcDatasourceModule[C: DecodeJson] extends LightweightDatasourceModule {

  type InitError = DE.InitializationError[Json]

  /** Returns the transactor configuration to use for the datasource having
    * the specified configuration or a list of errors describing why a
    * transactor could not be configured.
    */
  def transactorConfig(config: C): Either[NonEmptyList[String], TransactorConfig]

  def jdbcDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer, A](
      config: C,
      transactor: Transactor[F],
      rateLimiter: RateLimiting[F, A],
      byteStore: ByteStore[F],
      log: Logger)
      : Resource[F, Either[InitError, LightweightDatasourceModule.DS[F]]]

  ////

  def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer, A: Hash](
      config: Json,
      rateLimiter: RateLimiting[F, A],
      byteStore: ByteStore[F])(
      implicit ec: ExecutionContext)
      : Resource[F, Either[InitError, LightweightDatasourceModule.DS[F]]] = {

    val cfg0: Either[InitError, C] =
      config.as[C].fold(
        (err, c) =>
          Left(DE.malformedConfiguration[Json, InitError](
            kind,
            jString(Redacted),
            err)),
        Right(_))

    def validateConnection(timeout: FiniteDuration): ConnectionIO[Either[InitError, Unit]] =
      FC.isValid(timeout.toSeconds.toInt) map { v =>
        if (!v) Left(connectionInvalid(sanitizeConfig(config))) else Right(())
      }

    def liftF[X](fa: F[X]): EitherT[Resource[F, ?], InitError, X] =
      EitherT.right(Resource.liftF(fa))

    val init = for {
      cfg <- EitherT(cfg0.pure[Resource[F, ?]])

      xaCfg <- EitherT(transactorConfig(cfg).leftMap

      tag <- liftF(Sync[F].delay(Random.alphanumeric.take(6).mkString))

      debugId = s"datasource.$ident.$tag"

      awaitPool <- EitherT.right(awaitConnPool[F](s"$debugId.await", connPoolSize))
      xaPool <- EitherT.right(transactPool[F](s"$debugId.transact"))

      xa <- EitherT.right(hikariTransactor[F](cfg.connectionUri, connPoolSize, awaitPool, xaPool))

      _ <- liftF(validateConnection(cfg.connectionValidationTimeout).transact(xa) recover {
        case NonFatal(ex: Exception) =>
          Left(DE.connectionFailed[Json, InitError](kind, sanitizeConfig(config), ex))
      })

      slog <- liftF(Sync[F].delay(LoggerFactory(s"quasar.plugin.$debugId")))

      ds <- EitherT(jdbcDatasource(cfg, xa, rateLimiter, byteStore, slog))

      _ <- liftF(Sync[F].delay(slog.info(s"Initialized datasource $ident: tag = $tag, config = ${sanitizeConfig(config)}")))
    } yield ds

    init.value
  }

  private def ident: String = s"${kind.name.value}-v${kind.version.value}"

  private def awaitConnPool[F[_]](name: String, size: Int)(implicit F: Sync[F])
      : Resource[F, ExecutionContext] = {

    val alloc =
      F.delay(Executors.newFixedThreadPool(size, qc.NamedDaemonThreadFactory(name)))

    Resource.make(alloc)(es => F.delay(es.shutdown()))
      .map(ExecutionContext.fromExecutor)
  }

  private def connectionInvalid(c: Json): InitError =
    DE.connectionFailed[Json, InitError](
      kind, c, new RuntimeException("Database connection is invalid."))

  private def hikariTransactor[F[_]: Async: ContextShift](
      id: String,
      config: TransactorConfig,
      connectPool: ExecutionContext,
      xaBlocker: Blocker)
      : Resource[F, HikariTransactor[F]] = {

    HikariTransactor.initial[F](connectPool, xaBlocker) evalMap { xa =>
      xa.configure { ds =>
        Sync[F] delay {
          ds.setJdbcUrl(s"jdbc:$connUri")
          ds.setDriverClassName(driverFqcn)
          ds.setMaximumPoolSize(connPoolSize)
          xa
        }
      }
    }
  }

  private def transactPool[F[_]](name: String)(implicit F: Sync[F])
      : Resource[F, Blocker] = {

    val alloc =
      F.delay(Executors.newCachedThreadPool(qc.NamedDaemonThreadFactory(name)))

    Resource.make(alloc)(es => F.delay(es.shutdown()))
      .map(es => qc.Blocker(ExecutionContext.fromExecutor(es)))
  }
}
