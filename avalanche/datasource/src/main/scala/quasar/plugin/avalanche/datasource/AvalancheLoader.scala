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

import java.lang.CharSequence
import java.net.InetAddress
import java.nio.ByteBuffer
import java.sql.ResultSet
import java.text.ParsePosition
import java.time.{Duration, Period, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import java.util.UUID

import cats.effect.Resource
import cats.implicits._

import doobie._
import doobie.enum.JdbcType
import doobie.implicits._

import qdata.time.DateTimeInterval

import quasar.ScalarStages
import quasar.common.data.{QDataRValue, RValue}
import quasar.connector.QueryResult
import quasar.connector.datasource.BatchLoader
import quasar.plugin.jdbc._
import quasar.plugin.jdbc.datasource._

/** JdbcType(Avalanche type name): Class repr
  * -----------------------------------------
  * Char(char): java.lang.String
  * Char(nchar): java.lang.String
  * VarChar(varchar): java.lang.String
  * VarChar(nvarchar): java.lang.String
  * TinyInt(integer1): java.lang.Integer
  * SmallInt(smallint): java.lang.Integer
  * Integer(integer): java.lang.Integer
  * BigInt(bigint): java.lang.Long
  * Decimal(decimal): java.math.BigDecimal
  * Double(float): java.lang.Double
  * Date(ansidate): java.sql.Date
  * Time(time without time zone): java.sql.Time
  * Time(time with time zone): java.sql.Time
  * Time(time with local time zone): java.sql.Time
  * Timestamp(timestamp without time zone): java.sql.Timestamp
  * Timestamp(timestamp with time zone): java.sql.Timestamp
  * Timestamp(timestamp with local time zone): java.sql.Timestamp
  * VarChar(interval year to month): java.lang.String
  * VarChar(interval day to second): java.lang.String
  * Decimal(money): java.math.BigDecimal
  * Binary(ipv4): Array[Byte]
  * Binary(ipv6): Array[Byte]
  * Binary(uuid): Array[Byte]
  * Boolean(boolean): java.lang.Boolean
  *
  * @see https://docs.actian.com/avalanche/index.html#page/SQLLanguage%2F2._SQL_Data_Types.htm%23ww414616
  */
object AvalancheLoader {
  import java.sql.Types._

  type I = AvalancheHygiene.HygienicIdent

  val SupportedSqlTypes: Set[SqlType] =
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

          case ColumnSelection.All => fr0"*"
        }

        val sql =
          (fr"SELECT" ++ projections ++ fr" FROM" ++ dbObject0).query[Unit].sql

        val ps =
          FC.prepareStatement(
            sql,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY)

        val results =
          loggedRValueQuery(sql, ps, resultChunkSize, logHandler)(isSupported, unsafeRValue)

        results.map(QueryResult.parsed(QDataRValue, _, ScalarStages.Id))
    }

  def isSupported(sqlType: SqlType, avalancheType: VendorType): Boolean =
    SupportedSqlTypes(sqlType) || SupportedAvalancheTypes(avalancheType)

  def unsafeRValue(rs: ResultSet, col: ColumnNum, sqlType: SqlType, vendorType: VendorType): RValue = {
    def unlessNull[A](a: A)(f: A => RValue): RValue =
      if (a == null) null else f(a)

    (sqlType: @switch) match {
      case CHAR | NCHAR | NVARCHAR =>
        unlessNull(rs.getString(col))(RValue.rString(_))

      case VARCHAR =>
        unlessNull(rs.getString(col)) { s =>
          if (vendorType == IntervalYearToMonth)
            RValue.rInterval(unsafeParseYearToMonth(s))
          else if (vendorType == IntervalDayToSecond)
            RValue.rInterval(unsafeParseDayToSecond(s))
          else
            RValue.rString(s)
        }

      case TINYINT | SMALLINT | INTEGER | BIGINT =>
        unlessNull(rs.getLong(col))(RValue.rLong(_))

      case DOUBLE | FLOAT | REAL =>
        unlessNull(rs.getDouble(col))(RValue.rDouble(_))

      case DECIMAL | NUMERIC =>
        unlessNull(rs.getBigDecimal(col))(RValue.rNum(_))

      case BOOLEAN =>
        unlessNull(rs.getBoolean(col))(RValue.rBoolean(_))

      case DATE =>
        unlessNull(rs.getDate(col))(d => RValue.rLocalDate(d.toLocalDate))

      case TIME =>
        unlessNull(rs.getTime(col))(t => RValue.rLocalTime(t.toLocalTime))

      case TIMESTAMP =>
        unlessNull(rs.getTimestamp(col)) { ts =>
          RValue.rOffsetDateTime(ts.toInstant.atOffset(ZoneOffset.UTC))
        }

      case otherSql => vendorType match {
        // TODO: Could avoid the `InetAddress` allocation by parsing the bytes directly.
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

  ////

  private val NanosInSecond: Int = 1000 * 1000 * 1000

  // RANGE: -3652047 23:59:59.999999 to 3652047 23:59:59.999999
  // NB: We use 'u' (YEAR) in the pattern since the day value can overflow the day pattern
  private val DTSFormatter = DateTimeFormatter.ofPattern("u H:m:s")

  // Cribbed from https://github.com/precog/tectonic/blob/3ce1f15d4a3f1ca54678182c6f6e393312bbeca1/core/src/main/scala/tectonic/util/package.scala#L140
  private def unsafeParseInt(cs: CharSequence, start: Int, end: Int): Int = {
    // we store the inverse of the positive sum, to ensure we don't
    // incorrectly overflow on Int.MinValue. for positive numbers
    // this inverse sum will be inverted before being returned.
    var inverseSum: Int = 0
    var inverseSign: Int = -1
    var i: Int = start

    if (cs.charAt(i) == '-') {
      inverseSign = 1
      i += 1
    }

    while (i < end) {
      inverseSum = inverseSum * 10 - (cs.charAt(i).toInt - 48)
      i += 1
    }

    inverseSum * inverseSign
  }

  private def unsafeParseDayToSecond(cs: CharSequence): DateTimeInterval = {
    val p = new ParsePosition(0)
    val t = DTSFormatter.parse(cs, p)

    val nanos =
      if ((p.getIndex + 1) < cs.length && cs.charAt(p.getIndex) == '.') {
        val fraction = unsafeParseInt(cs, p.getIndex + 1, cs.length)
        if (fraction == 0) 0L else ((1.0 / fraction) * NanosInSecond).toLong
      } else {
        0L
      }

    DateTimeInterval(
      // We parsed the days value using the YEAR field to avoid overflow
      Period.of(0, 0, t.get(ChronoField.YEAR)),
      Duration.ofSeconds(t.getLong(ChronoField.SECOND_OF_DAY), nanos))
  }

  // RANGE: -9999-11 to 9999-11
  private def unsafeParseYearToMonth(cs: CharSequence): DateTimeInterval = {
    val len: Int = cs.length
    var sign: Int = 1
    var y: Int = 0
    var h: Int = 1

    if (cs.charAt(0) == '-') {
      sign = -1
      y = 1
    }

    while (h < len && cs.charAt(h) != '-') {
      h += 1
    }

    DateTimeInterval.ofPeriod(Period.of(
      sign * unsafeParseInt(cs, y, h),
      sign * unsafeParseInt(cs, h + 1, len),
      0))
  }
}
