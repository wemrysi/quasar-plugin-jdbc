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

import scala._, Predef._

import cats.implicits._

import doobie.ConnectionIO

import quasar.api.ColumnType

object MaskInterpreter {
  /** Returns a function that attempts to interpret an initial `Mask` stage
    * into a `ColumnSelection` given a function that returns the quasar type
    * of each column in the database.
    *
    * @param f returns the type of each column in the given table and optional schema
    */
  def apply(
      hygiene: Hygiene)(
      f: (hygiene.HygienicIdent, Option[hygiene.HygienicIdent]) => ConnectionIO[Map[ColumnName, ColumnType.Scalar]])
      : MaskInterpreter[hygiene.HygienicIdent] = {
    case (table, schema, stages) =>
      f(table, schema) map { scalars =>
        val (selection, nextStages) =
          MaskedScalarColumns((n, ts) => scalars.get(n).exists(ts(_)))(stages)

        (table, schema, selection.map(hygiene.hygienicIdent(_)), nextStages)
      }
  }
}
