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

import org.specs2.mutable.Specification

import quasar.{IdStatus, ScalarStage, ScalarStages}
import quasar.api.ColumnType
import quasar.common.CPath

object MaskedScalarColumnsSpec extends Specification {
  import ScalarStage._

  type IsDef = (ColumnName, NonEmptySet[ColumnType.Scalar]) => Boolean

  val allDef: IsDef = (_, _) => true

  def cp(s: String): CPath = CPath.parse(s)

  def noId(ss: ScalarStage*): ScalarStages =
    ScalarStages(IdStatus.ExcludeId, ss.toList)

  "no Mask stage results in all columns and unmodified stages" >> {
    val ss = noId(Wrap("foo"), Project(cp(".foo.bar")))
    MaskedScalarColumns(allDef)(ss) must_=== (ColumnSelection.All -> ss)
  }

  "any id inclusion results in all columns and unmodified stages" >> {
    val mask = Mask(Map(cp(".foo") -> Set(ColumnType.String)))
    val idOnly = ScalarStages(IdStatus.IdOnly, List(mask))
    val includeId = ScalarStages(IdStatus.IncludeId, List(mask))

    MaskedScalarColumns(allDef)(idOnly) must_=== (ColumnSelection.All -> idOnly)
    MaskedScalarColumns(allDef)(includeId) must_=== (ColumnSelection.All -> includeId)
  }

  "empty mask stage results in none seleced" >> {
    MaskedScalarColumns(allDef)(noId(Mask(Map()))) must_=== (ColumnSelection.None -> ScalarStages.Id)
  }

  "defined column is selected and undefined elided" >> {
    val p: IsDef = {
      case (n, _) if n.asString.startsWith("x") => true
      case _ => false
    }

    val mm = Map(
      cp(".x1") -> ColumnType.Top,
      cp(".z") -> ColumnType.Top,
      cp(".x2") -> ColumnType.Top,
      cp(".y") -> ColumnType.Top)

    val (selected, nextStages) = MaskedScalarColumns(p)(noId(Mask(mm)))

    nextStages must_=== ScalarStages.Id

    selected must beLike {
      case ColumnSelection.Explicit(cs) =>
        cs.toList must contain(exactly(ColumnName("x1"), ColumnName("x2")))
    }
  }

  "inferred non-scalar column is elided" >> {
    val mm = Map(
      cp(".a") -> ColumnType.Top,
      cp(".b.c") -> ColumnType.Top,
      cp(".q[0]") -> ColumnType.Top)

    val (selected, nextStages) = MaskedScalarColumns(allDef)(noId(Mask(mm)))

    nextStages must_=== ScalarStages.Id

    selected must beLike {
      case ColumnSelection.Explicit(cs) =>
        cs.toList must contain(exactly((ColumnName("a"))))
    }
  }

  "column that doesn't begin with field is elided" >> {
    val mm = Map(
      cp("[1]") -> ColumnType.Top,
      cp(".c") -> ColumnType.Top)

    val (selected, nextStages) = MaskedScalarColumns(allDef)(noId(Mask(mm)))

    nextStages must_=== ScalarStages.Id

    selected must beLike {
      case ColumnSelection.Explicit(cs) =>
        cs.toList must contain(exactly((ColumnName("c"))))
    }
  }

  "all columns elided results in none selected" >> {
    val mm = Map(
      cp(".a") -> ColumnType.Top,
      cp(".b") -> ColumnType.Top,
      cp(".c") -> ColumnType.Top,
      cp(".d") -> ColumnType.Top)

    val noneDef: IsDef = (_, _) => false

    MaskedScalarColumns(noneDef)(noId(Mask(mm))) must_=== (ColumnSelection.None -> ScalarStages.Id)
  }
}
