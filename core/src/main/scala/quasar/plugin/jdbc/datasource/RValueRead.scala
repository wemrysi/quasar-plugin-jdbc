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

import slamdata.Predef._

import quasar.plugin.jdbc.VendorType

import scala.collection.immutable.BitSet

import java.sql.{ResultSet, ResultSetMetaData}

import doobie.enum.JdbcType
import doobie.util.Read

import quasar.common.data._

object RValueRead {
  /** Returns a `Read[RValue]` for the `ResultSet` that produced the provided
    * `ResultSetMetaData`.
    *
    * @param meta the metadata for the ResultSet being read
    * @param isSupported returns whether a column of the specified JDBC and
    *                    vendor type can be represented as an RValue.
    * @param unsafeRValue a function that extracts an RValue from the specified
    *                     column in the provided `ResultSet`, having the given
    *                     SQL type and lowercase vendor type name. Will only be
    *                     invoked for types where `isSupported` returns true. If the
    *                     `ResultSet` column result is `null`, this function
    *                     should return `null`.
    */
  def apply(
      meta: ResultSetMetaData,
      isSupported: (SqlType, VendorType) => Boolean,
      unsafeRValue: (ResultSet, ColumnNum, SqlType, VendorType) => RValue)
      : Read[RValue] = {

    val size: Int = meta.getColumnCount
    val sqlTypes: Array[SqlType] = new Array[SqlType](size + 1)
    val vendorTypes: Array[VendorType] = new Array[VendorType](size + 1)
    val columnLabels: Array[String] = new Array[String](size + 1)
    var structure: Map[String, RValue] = Map.empty[String, RValue]
    var unsupportedColumns: BitSet = BitSet.empty

    var i = 1

    while (i <= size) {
      sqlTypes(i) = meta.getColumnType(i)
      vendorTypes(i) = meta.getColumnTypeName(i).toLowerCase
      columnLabels(i) = meta.getColumnLabel(i)

      structure =
        if (isSupported(sqlTypes(i), vendorTypes(i))) {
          structure.updated(columnLabels(i), RValue.rNull())
        } else {
          unsupportedColumns += i
          structure.updated(
            columnLabels(i),
            RValue.rString(unsupportedColumnTypeMsg(
              JdbcType.fromInt(sqlTypes(i)),
              vendorTypes(i))))
        }

      i += 1
    }

    def unsafeGet(rs: ResultSet, `_`: Int): RValue = {
      var c: Int = 1
      var rv: RValue = null
      var row: Map[String, RValue] = structure

      while (c <= size) {
        if (!unsupportedColumns(c)) {
          rv = unsafeRValue(rs, c, sqlTypes(c), vendorTypes(c))

          if (rv != null) {
            row = row.updated(columnLabels(c), rv)
          }
        }

        c += 1
      }

      RValue.rObject(row)
    }

    new Read[RValue](Nil, unsafeGet)
  }
}
