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

import quasar.plugin.avalanche._

import scala._, Predef._
import scala.annotation.switch

import java.net.InetAddress
import java.nio.ByteBuffer
import java.sql.ResultSet
import java.time.ZoneOffset
import java.util.UUID

import cats.effect.Resource
import cats.implicits._

import doobie._
import doobie.enum.JdbcType
import doobie.implicits._

import quasar.ScalarStages
import quasar.common.data.{QDataRValue, RValue}
import quasar.connector.QueryResult
import quasar.connector.datasource.BatchLoader
import quasar.plugin.jdbc._
import quasar.plugin.jdbc.datasource._

/*
{
  "a": "<UNSUPPORTED COLUMN TYPE: Char(char): java.lang.String>",
  "b": "<UNSUPPORTED COLUMN TYPE: Char(nchar): java.lang.String>",
  "c": "<UNSUPPORTED COLUMN TYPE: VarChar(varchar): java.lang.String>",
  "d": "<UNSUPPORTED COLUMN TYPE: VarChar(nvarchar): java.lang.String>",
  "e": "<UNSUPPORTED COLUMN TYPE: TinyInt(integer1): java.lang.Integer>",
  "f": "<UNSUPPORTED COLUMN TYPE: SmallInt(smallint): java.lang.Integer>",
  "g": "<UNSUPPORTED COLUMN TYPE: Integer(integer): java.lang.Integer>",
  "h": "<UNSUPPORTED COLUMN TYPE: BigInt(bigint): java.lang.Long>",
  "i": "<UNSUPPORTED COLUMN TYPE: Decimal(decimal): java.math.BigDecimal>",
  "j": "<UNSUPPORTED COLUMN TYPE: Double(float): java.lang.Double>",
  "l": "<UNSUPPORTED COLUMN TYPE: Date(ansidate): java.sql.Date>",
  "m": "<UNSUPPORTED COLUMN TYPE: Time(time without time zone): java.sql.Time>",
  "n": "<UNSUPPORTED COLUMN TYPE: Time(time with time zone): java.sql.Time>",
  "o": "<UNSUPPORTED COLUMN TYPE: Time(time with local time zone): java.sql.Time>",
  "p": "<UNSUPPORTED COLUMN TYPE: Timestamp(timestamp without time zone): java.sql.Timestamp>",
  "q": "<UNSUPPORTED COLUMN TYPE: Timestamp(timestamp with time zone): java.sql.Timestamp>",
  "r": "<UNSUPPORTED COLUMN TYPE: Timestamp(timestamp with local time zone): java.sql.Timestamp>",
  "s": "<UNSUPPORTED COLUMN TYPE: VarChar(interval year to month): java.lang.String>",
  "t": "<UNSUPPORTED COLUMN TYPE: VarChar(interval day to second): java.lang.String>",
  "u": "<UNSUPPORTED COLUMN TYPE: Decimal(money): java.math.BigDecimal>",
  "v": "<UNSUPPORTED COLUMN TYPE: Binary(ipv4): [B>",
  "w": "<UNSUPPORTED COLUMN TYPE: Binary(ipv6): [B>",
  "x": "<UNSUPPORTED COLUMN TYPE: Binary(uuid): [B>",
  "y": "<UNSUPPORTED COLUMN TYPE: Boolean(boolean): java.lang.Boolean>"
}
*/

// TODO: Log unsupported columns
object AvalancheLoader {
  import java.sql.Types._

  type I = AvalancheHygiene.HygienicIdent

  val SupportedSqlTypes: Set[Int] =
    Set(
      BIGINT,
      BOOLEAN,
      CHAR,
      DATE,
      DECIMAL,
      DOUBLE,
      FLOAT,
      INTEGER,
      NCHAR,
      NUMERIC,
      NVARCHAR,
      REAL,
      SMALLINT,
      TIME,
      TIMESTAMP,
      TINYINT,
      VARCHAR)

  // VARCHAR
  val IntervalYearToMonth = "interval year to month"
  val IntervalDayToSecond = "interval day to second"
  // DECIMAL
  val Money = "money"
  // BINARY
  val IPv4 = "ipv4"
  val IPv6 = "ipv6"
  val UUID = "uuid"

  val SupportedAvalancheTypes: Set[VendorType] =
    Set(IntervalYearToMonth, IntervalDayToSecond, Money, IPv4, IPv6, UUID)

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

        val sql =
          (fr"SELECT" ++ projections ++ fr"FROM" ++ dbObject0).query[Unit].sql

        val ps =
          FC.prepareStatement(
            sql,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY)

        val results =
          loggedRValueQuery(sql, ps, resultChunkSize, logHandler)(
            isSupported,
            unsafeRValue)

        results.map(QueryResult.parsed(QDataRValue, _, ScalarStages.Id))
    }

  def isSupported(sqlType: SqlType, avalancheType: VendorType): Boolean =
    SupportedSqlTypes(sqlType) || SupportedAvalancheTypes(avalancheType)

  // TODO: Error handling?
  def unsafeRValue(rs: ResultSet, col: ColumnNum, sqlType: SqlType, vendorType: VendorType): RValue = {
    def unlessNull[A](a: A)(f: A => RValue): RValue =
      if (a == null) null else f(a)

    (sqlType: @switch) match {
      // TODO: handle intervals and interpret into DateTimeInterval
      case CHAR | NCHAR | NVARCHAR | VARCHAR => unlessNull(rs.getString(col))(RValue.rString(_))

      case TINYINT | SMALLINT | INTEGER | BIGINT => unlessNull(rs.getLong(col))(RValue.rLong(_))

      case DOUBLE | FLOAT | REAL => unlessNull(rs.getDouble(col))(RValue.rDouble(_))

      case DECIMAL | NUMERIC => unlessNull(rs.getBigDecimal(col))(RValue.rNum(_))

      case BOOLEAN => unlessNull(rs.getBoolean(col))(RValue.rBoolean(_))

      case DATE => unlessNull(rs.getDate(col))(d => RValue.rLocalDate(d.toLocalDate))

      // TODO: Timezone handling?
      case TIME =>
        unlessNull(rs.getTime(col))(t => RValue.rLocalTime(t.toLocalTime))

      // TODO: Timezone handling?
      case TIMESTAMP =>
        unlessNull(rs.getTimestamp(col)) { ts =>
          RValue.rOffsetDateTime(ts.toInstant.atOffset(ZoneOffset.UTC))
        }

      case otherSql => vendorType match {
//      case IntervalYearToMonth => ???

//      case IntervalDayToSecond => ???

        // TODO: Parsing the bytes directly would eliminate the `InetAddress` allocation
        case IPv4 | IPv6 =>
          unlessNull(rs.getBytes(col)) { bs =>
            RValue.rString(InetAddress.getByAddress(bs).getHostAddress)
          }

        case UUID =>
          unlessNull(rs.getBytes(col)) { bs =>
            val bb = ByteBuffer.wrap(bs)
            RValue.rString((new UUID(bb.getLong, bb.getLong)).toString)
          }

        case otherVendor =>
          RValue.rString(unsupportedColumnTypeMsg(JdbcType.fromInt(otherSql), otherVendor))
      }
    }
  }
}
