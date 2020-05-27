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

import scala.{Int, Nil}

import java.sql.{ResultSet, ResultSetMetaData}

import doobie.util.Read

import quasar.common.data._

// TODO: Generalize the type conversion so this is usable by other impls.
object AvalancheRValueRead {
  /** Returns a `Read[RValue]` for the `ResultSet` that produced the provided
    * `ResultSetMetaData`.
    */
  def apply(meta: ResultSetMetaData): Read[RValue] =
    new Read[RValue](Nil, readRValue(meta, _, _))

  /** Reads an `RValue`, beginning at `startColumn`, from the current row of
    * the provided `ResultSet`.
    *
    * @param meta the metadata for the provided `ResultSet`
    */
  def readRValue(meta: ResultSetMetaData, rs: ResultSet, startColumn: Int): RValue =
    CNull
}
