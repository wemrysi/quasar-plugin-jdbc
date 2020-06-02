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

import slamdata.Predef._

import quasar.plugin.avalanche._

import scala.Predef.implicitly
import scala.concurrent.duration._
import scala.math
import scala.reflect.ClassTag

import java.lang.System
import java.sql.{PreparedStatement, ResultSet}

import cats.~>
import cats.effect.Resource
import cats.implicits._

import doobie._
import doobie.enum.JdbcType
import doobie.implicits._
import doobie.util.log._

import fs2.{Chunk, Stream}

import quasar.ScalarStages
import quasar.common.data.{QDataRValue, RValue}
import quasar.connector.QueryResult
import quasar.connector.datasource.BatchLoader
import quasar.plugin.jdbc._
import quasar.plugin.jdbc.datasource._

/*
{
  "a": "<UNSUPPORTED COLUMN TYPE: Char(char)>",
  "b": "<UNSUPPORTED COLUMN TYPE: Char(nchar)>",
  "c": "<UNSUPPORTED COLUMN TYPE: VarChar(varchar)>",
  "d": "<UNSUPPORTED COLUMN TYPE: VarChar(nvarchar)>",
  "e": "<UNSUPPORTED COLUMN TYPE: TinyInt(integer1)>",
  "f": "<UNSUPPORTED COLUMN TYPE: SmallInt(smallint)>",
  "g": "<UNSUPPORTED COLUMN TYPE: Integer(integer)>",
  "h": "<UNSUPPORTED COLUMN TYPE: BigInt(bigint)>",
  "i": "<UNSUPPORTED COLUMN TYPE: Decimal(decimal)>",
  "j": "<UNSUPPORTED COLUMN TYPE: Double(float)>",
  "l": "<UNSUPPORTED COLUMN TYPE: Date(ansidate)>",
  "m": "<UNSUPPORTED COLUMN TYPE: Time(time without time zone)>",
  "n": "<UNSUPPORTED COLUMN TYPE: Time(time with time zone)>",
  "o": "<UNSUPPORTED COLUMN TYPE: Time(time with local time zone)>",
  "p": "<UNSUPPORTED COLUMN TYPE: Timestamp(timestamp without time zone)>",
  "q": "<UNSUPPORTED COLUMN TYPE: Timestamp(timestamp with time zone)>",
  "r": "<UNSUPPORTED COLUMN TYPE: Timestamp(timestamp with local time zone)>",
  "s": "<UNSUPPORTED COLUMN TYPE: VarChar(interval year to month)>",
  "t": "<UNSUPPORTED COLUMN TYPE: VarChar(interval day to second)>",
  "u": "<UNSUPPORTED COLUMN TYPE: Decimal(money)>",
  "v": "<UNSUPPORTED COLUMN TYPE: Binary(ipv4)>",
  "w": "<UNSUPPORTED COLUMN TYPE: Binary(ipv6)>",
  "x": "<UNSUPPORTED COLUMN TYPE: Binary(uuid)>",
  "y": "<UNSUPPORTED COLUMN TYPE: Boolean(boolean)>"
}
*/

// TODO: Log unsupported columns
// TODO: Extract general stuff to quasar.plugin.jdbc.datasource
object AvalancheLoader {
  type I = AvalancheHygiene.HygienicIdent

  def apply(
      discovery: JdbcDiscovery,
      logHandler: LogHandler,
      resultChunkSize: Int)
      : MaskedLoader[ConnectionIO, I] =
    BatchLoader.Full[Resource[ConnectionIO, ?], (I, Option[I], ColumnSelection[I]), QueryResult[ConnectionIO]] {
      case (table, schema, columns) =>
        val dbObject0 =
          schema.fold(table.fr0)(_.fr0 ++ Fragment.const0(".") ++ table.fr0)

        val projections = columns match {
          case ColumnSelection.Explicit(idents) =>
            idents.map(_.fr0).intercalate(fr",")

          case ColumnSelection.All => fr"*"
        }

        val query =
          fr"SELECT" ++ projections ++ fr"FROM" ++ dbObject0

        val load = executeLogged(
          query.query[Unit].sql,
          FPS.setFetchSize(resultChunkSize) *> FPS.executeQuery,
          avalancheRValues(resultChunkSize),
          logHandler)

        load.map(QueryResult.parsed(QDataRValue, _, ScalarStages.Id))
    }

  val SupportedJdbcTypes: Set[JdbcType] = {
    import JdbcType._

    Set(
      BigInt,
      Boolean,
      Char,
      Date,
      Decimal,
      Double,
      Float,
      Integer,
      NChar,
      Numeric,
      NVarChar,
      Real,
      SmallInt,
      Time,
      TimeWithTimezone,
      Timestamp,
      TimestampWithTimezone,
      TinyInt,
      VarChar)
  }

  val SupportedAvalancheTypes: Set[String] =
    Set("INTERVAL YEAR TO MONTH", "INTERVAL DAY TO SECOND", "MONEY", "IPV4", "IPV6", "UUID")

  def isSupported(jdbcType: JdbcType, avalancheTypeName: String): Boolean =
    SupportedJdbcTypes(jdbcType) || SupportedAvalancheTypes(avalancheTypeName)

  def unsafeRValue(rs: ResultSet, col: Int, jdbcType: JdbcType, vendorName: String): RValue = {
    import JdbcType._

    def unlessNull[A](a: A)(f: A => RValue): RValue =
      if (a == null) null else f(a)

    jdbcType match {
      case Char | NChar | NVarChar | VarChar => unlessNull(rs.getString(col))(RValue.rString(_))

      case TinyInt | SmallInt | Integer | BigInt => unlessNull(rs.getLong(col))(RValue.rLong(_))

      case Double | Float | Real => unlessNull(rs.getDouble(col))(RValue.rDouble(_))

      case Decimal | Numeric => unlessNull(rs.getBigDecimal(col))(RValue.rNum(_))

      case Boolean => unlessNull(rs.getBoolean(col))(RValue.rBoolean(_))

//    case Date => ???

//    case Time => ???

//    case TimeWithTimezone => ???

//    case Timestamp => ???

//    case TimestampWithTimeZone => ???
    }
  }


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

  def resultStream[A: ClassTag: Read](chunkSize: Int): Stream[ResultSetIO, A] =
    Stream.repeatEval(getNextChunk(chunkSize))
      .takeWhile(_.nonEmpty)
      .flatMap(Stream.chunk(_))

  ////

  private def avalancheRValues(chunkSize: Int): Stream[ResultSetIO, RValue] =
    Stream.eval(FRS.getMetaData) flatMap { meta =>
      val read = AvalancheRValueRead(meta, (_, _) => false, (_, _, _, _) => null)
      resultStream(chunkSize)(implicitly[ClassTag[RValue]], read)
    }

  private def executeLogged[A](
      sql: String,
      execute: PreparedStatementIO[ResultSet],
      results: Stream[ResultSetIO, A],
      logHandler: LogHandler)
      : Resource[ConnectionIO, Stream[ConnectionIO, A]] = {

    def diff(a: Long, b: Long) = FiniteDuration(math.abs(a - b), NANOSECONDS)
    def log(e: LogEvent) = FC.delay(logHandler.unsafeRun(e))
    val now = FC.delay(System.nanoTime)

    for {
      t0 <- Resource.liftF(now)

      ps <- prepared(sql)

      er <- Resource.make(FC.embed(ps, execute))(FC.embed(_, FRS.close)).attempt

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

  private def prepared(sql: String): Resource[ConnectionIO, PreparedStatement] =
    Resource.make(
      FC.prepareStatement(
        sql,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY))(
      FC.embed(_, FPS.close))

  private def rsEmbedK(rs: ResultSet): ResultSetIO ~> ConnectionIO =
    λ[ResultSetIO ~> ConnectionIO](FC.embed(rs, _))
}
