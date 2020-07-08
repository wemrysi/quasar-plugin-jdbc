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

package quasar.plugin.jdbc.destination

import quasar.plugin.jdbc._

import scala.{io => _, _}
import scala.concurrent.duration.MILLISECONDS
import scala.util.Either

import java.time.Duration

import cats.data.NonEmptyList
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._

import fs2.Stream

import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import org.slf4s.Logger

import quasar.api.Column
import quasar.api.resource.ResourcePath
import quasar.connector.{MonadResourceErr, ResourceError}

object JdbcCreateSink {
  type JdbcSink[F[_], I, T] =
    (Either[I, (I, I)], NonEmptyList[(I, T)], Stream[F, Byte]) => Stream[F, Unit]

  def apply[F[_]: MonadResourceErr: Sync, T](
      hygiene: Hygiene,
      logger: Logger)(
      jdbcSink: JdbcSink[F, hygiene.HygienicIdent, T])(
      implicit timer: Timer[F])
      : (ResourcePath, NonEmptyList[Column[T]], Stream[F, Byte]) => Stream[F, Unit] = { (path, columns, bytes) =>

    import hygiene._

    val log = Slf4jLogger.getLoggerFromSlf4j[F](logger.underlying)

    Stream.force(for {
      dbo <- resourcePathRef(path) match {
        case Some(ref) => ref.pure[F]
        case _ => MonadResourceErr[F].raiseError(ResourceError.notAResource(path))
      }

      hygienicRef = dbo.bimap(
        hygienicIdent(_),
        { case (f, s) => (hygienicIdent(f), hygienicIdent(f)) })

      dboDebug = dbo.fold(
        _.asString,
        { case (f, s) => s"${f.asString}.${s.asString}" })

      totalBytes <- Ref[F].of(0L)
      startAt <- timer.clock.monotonic(MILLISECONDS)

      instrumentedBytes =
        bytes.chunks
          .evalTap(c =>
            totalBytes.update(_ + c.size) >>
              log.trace(s"[$dboDebug] Sending ${c.size} bytes"))
          .flatMap(Stream.chunk(_))

      _ <- log.debug(s"[$dboDebug] Ingest started")

      ingestSucceeded = for {
        endAt <- timer.clock.monotonic(MILLISECONDS)
        elapsed = Duration.ofMillis(endAt - startAt)
        total <- totalBytes.get
        _ <- log.debug(s"[$dboDebug] Ingest succeeded, loaded $total bytes in $elapsed")
      } yield ()

      hygienicColumns = columns.map(c => (hygienicIdent(Ident(c.name)), c.tpe))

      sunk = jdbcSink(hygienicRef, hygienicColumns, instrumentedBytes) ++ Stream.eval_(ingestSucceeded)

      out = sunk onError {
        case t => Stream.eval_(for {
          failedAt <- timer.clock.monotonic(MILLISECONDS)
          elapsed = Duration.ofMillis(failedAt - startAt)
          progress <- totalBytes.get
          _ <- log.debug(t)(s"[$dboDebug] Ingest failed after $progress bytes in $elapsed")
        } yield ())
      }
    } yield out)
  }
}
