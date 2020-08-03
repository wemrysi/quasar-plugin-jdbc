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

import quasar.plugin.jdbc._

import scala.Boolean
import java.sql.ResultSet

import quasar.common.data.RValue

/** A partial function for reading `RValue`s from a JDBC `ResultSet`. */
trait RValueColumn {
  /** Returns whether a column having the given SQL and vendor types is representable
    * as an `RValue`.
    */
  def isSupported(sqlType: SqlType, mariadbType: VendorType): Boolean

  /** Returns the value of column `col` from the current row of the `ResultSet` as
    * an `RValue`.
    *
    * "unsafe" as this is only safe to call when
    * `isSupported(sqlType, vendorType) == true` and will throw an exception otherwise.
    */
  def unsafeRValue(rs: ResultSet, col: ColumnNum, sqlType: SqlType, vendorType: VendorType): RValue
}
