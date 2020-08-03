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

import scala._, Predef._
import scala.concurrent.duration._
import scala.math
import scala.reflect.ClassTag

import java.lang.{System, String}
import java.sql.{PreparedStatement, ResultSet}

import cats.~>
import cats.effect.Resource
import cats.implicits._

import doobie._
import doobie.enum.JdbcType
import doobie.implicits._
import doobie.util.log._

import fs2.{Chunk, Stream}

import quasar.common.data.RValue
import quasar.ScalarStages
import quasar.connector.QueryResult
import quasar.connector.datasource.BatchLoader

package object datasource {
  // 1-indexed ResultSet column number
  type ColumnNum = Int

  // A constant identifying a SQL type from java.sql.Types
  type SqlType = Int

  type JdbcLoader[F[_], A] = BatchLoader[Resource[F, ?], JdbcLoader.Args[A], QueryResult[F]]

  type MaskInterpreter[A] =
    JdbcLoader.Args[A] => ConnectionIO[(A, Option[A], ColumnSelection[A], ScalarStages)]

  def getNextChunk[A: ClassTag](chunkSize: Int)(implicit A: Read[A]): ResultSetIO[Chunk[A]] =
    FRS raw { rs =>
      val c = new Array[A](chunkSize)
      var n = 0

      while (n < chunkSize && rs.next) {
        c(n) = A.unsafeGet(rs, 1)
        n += 1
      }

      Chunk.boxed(c, 0, n)
    }

  /** Returns a stream of the results of executing a query, logging execution
    * via the specified `LogHandler`.
    *
    * @param sql the SQL query being executed
    * @param prepare a prepared statement representing the `sql`
    * @param execute a program that executes a prepared statement
    * @param results a stream of values produced from a `ResultSet`
    * @param logHandler used to handle `LogEvent`s during execution
    */
  def loggedQuery[A](
      sql: String,
      prepare: ConnectionIO[PreparedStatement],
      execute: PreparedStatementIO[ResultSet],
      results: Stream[ResultSetIO, A],
      logHandler: LogHandler)
      : Resource[ConnectionIO, Stream[ConnectionIO, A]] = {

    def diff(a: Long, b: Long) = FiniteDuration(math.abs(a - b), NANOSECONDS)
    def log(e: LogEvent) = FC.delay(logHandler.unsafeRun(e))
    val now = FC.delay(System.nanoTime)

    val prepR =
      Resource.make(prepare)(FC.embed(_, FPS.close))

    def execR(ps: PreparedStatement) =
      Resource.make(FC.embed(ps, execute))(FC.embed(_, FRS.close))

    for {
      t0 <- Resource.liftF(now)

      er <- prepR.flatMap(execR).attempt

      t1 <- Resource.liftF(now)

      rs <- er.liftTo[Resource[ConnectionIO, ?]] onError {
        case e => Resource.liftF(log(ExecFailure(sql, Nil, diff(t1, t0), e)))
      }

      as = results.onFinalize(FRS.close).translate(rsEmbedK(rs)) onError {
        case e => Stream.eval(for {
          t2 <- now
          _ <- log(ProcessingFailure(sql, Nil, diff(t1, t0), diff(t2, t1), e))
        } yield ())
      }

      logSuccess = now flatMap { t2 =>
        log(Success(sql, Nil, diff(t1, t0), diff(t2, t1)))
      }
    } yield as ++ Stream.eval_(logSuccess)
  }

  def loggedRValueQuery(
      sql: String,
      prepare: ConnectionIO[PreparedStatement],
      resultChunkSize: Int,
      logHandler: LogHandler)(
      isSupported: (SqlType, VendorType) => Boolean,
      unsafeRValue: (ResultSet, ColumnNum, SqlType, VendorType) => RValue)
      : Resource[ConnectionIO, Stream[ConnectionIO, RValue]] = {

    val rvalues =
      Stream.eval(FRS.getMetaData) flatMap { meta =>
        val read = RValueRead(meta, isSupported, unsafeRValue)
        resultStream(resultChunkSize)(implicitly[ClassTag[RValue]], read)
      }

    val exec = FPS.setFetchSize(resultChunkSize) *> FPS.executeQuery

    loggedQuery(sql, prepare, exec, rvalues, logHandler)
  }

  def resultStream[A: ClassTag: Read](chunkSize: Int): Stream[ResultSetIO, A] =
    Stream.repeatEval(getNextChunk(chunkSize))
      .takeWhile(_.nonEmpty)
      .flatMap(Stream.chunk(_))

  def unsupportedColumnTypeMsg(jdbcType: JdbcType, vendorName: String): String =
    s"<UNSUPPORTED COLUMN TYPE: ${jdbcType}($vendorName)>"

  ////

  private def rsEmbedK(rs: ResultSet): ResultSetIO ~> ConnectionIO =
    λ[ResultSetIO ~> ConnectionIO](FC.embed(rs, _))
}
