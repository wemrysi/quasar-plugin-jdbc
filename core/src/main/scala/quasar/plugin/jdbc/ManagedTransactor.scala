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

import java.lang.String
import java.util.concurrent.Executors

import scala.{Int, StringContext}
import scala.concurrent.ExecutionContext

import cats.effect._

import com.zaxxer.hikari.HikariConfig

import doobie.Transactor
import doobie.hikari.HikariTransactor

import quasar.concurrent.NamedDaemonThreadFactory

object ManagedTransactor {
  def apply[F[_]: Async: ContextShift](
      name: String,
      config: TransactorConfig)
      : Resource[F, Transactor[F]] = {

    import JdbcDriverConfig._

    val hikariConfig = Async[F] delay {
      val c = new HikariConfig()

      c.setPoolName(s"$name.pool")

      config.driverConfig match {
        case JdbcDataSourceConfig(className, props) =>
          c.setDataSourceClassName(className)
          props.foreach { case (k, v) => c.addDataSourceProperty(k, v) }

        case JdbcDriverManagerConfig(url, className) =>
          c.setJdbcUrl(url.toString)
          className.foreach(c.setDriverClassName)
      }

      c.setMaximumPoolSize(config.connectionMaxConcurrency)
      c.setReadOnly(config.connectionReadOnly)
      c.setConnectionTimeout(config.connectionTimeout.toMillis)
      c.setValidationTimeout(config.connectionValidationTimeout.toMillis)
      c.setMaxLifetime(config.connectionMaxLifetime.toMillis)

      // Fail initialization unless able to obtain a valid connection
      c.setInitializationFailTimeout(1)

      c
    }

    for {
      hc <- Resource.liftF(hikariConfig)

      awaiting <- awaitPool[F](s"$name.await", config.connectionMaxConcurrency)
      transacting <- transactPool[F](s"$name.transact")

      xa <-
        HikariTransactor
          .fromHikariConfig[F](hc, awaiting, transacting)
          .mapK(transacting.blockOnK[F]) // initial connection test blocks

    } yield xa
  }

  /** Returns an `ExecutionContext` of size `size` suitable for awaiting JDBC
    * connections.
    */
  def awaitPool[F[_]](name: String, size: Int)(implicit F: Sync[F])
      : Resource[F, ExecutionContext] = {
    val alloc =
      F.delay(Executors.newFixedThreadPool(size, NamedDaemonThreadFactory(name)))

    Resource.make(alloc)(es => F.delay(es.shutdown()))
      .map(ExecutionContext.fromExecutor)
  }

  /** Returns an unbounded `Blocker` suitable for executing JDBC transactions. */
  def transactPool[F[_]](name: String)(implicit F: Sync[F]): Resource[F, Blocker] = {
    val alloc =
      F.delay(Executors.newCachedThreadPool(NamedDaemonThreadFactory(name)))

    Resource.make(alloc)(es => F.delay(es.shutdown()))
      .map(es => Blocker.liftExecutionContext(ExecutionContext.fromExecutor(es)))
  }
}
