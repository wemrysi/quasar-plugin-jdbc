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

import cats.data.NonEmptySet
import cats.implicits._

import quasar.{IdStatus, ScalarStage, ScalarStages}
import quasar.api.ColumnType
import quasar.common.{CPath, CPathField}

object MaskedScalarColumns {
  /** Returns the `ColumnSelection` determined by the given `ScalarStages`.
    *
    * @param isDefined returns true if the named column exists in the database
    *                  and has a type compatible with any of the given scalar
    *                  types
    */
  def apply(
      isDefined: (ColumnName, NonEmptySet[ColumnType.Scalar]) => Boolean)(
      scalarStages: ScalarStages)
      : (ColumnSelection[ColumnName], ScalarStages) = {

    def isDefinedForAny(c: ColumnName, ts: Set[ColumnType]): Boolean = {
      val scalars = ts.toList collect {
        case s: ColumnType.Scalar => s
      }

      scalars match {
        case h :: t => isDefined(c, NonEmptySet.of(h, t: _*))
        case Nil => false
      }
    }

    scalarStages match {
      case ScalarStages(excludeId @ IdStatus.ExcludeId, ScalarStage.Mask(m) :: rest) =>
        val masked =
          m.toList collect {
            // elide all other paths as they imply non-object (array index) or
            // non-scalar (nested structure) values
            case ((CPath(CPathField(f)), types)) if isDefinedForAny(ColumnName(f), types) =>
              ColumnName(f)
          }

        val selection =
          masked.toNel
            .map(ColumnSelection.Explicit(_))
            .getOrElse(ColumnSelection.None)

        (selection, ScalarStages(excludeId, rest))

      case other =>
        (ColumnSelection.All, other)
    }
  }
}
