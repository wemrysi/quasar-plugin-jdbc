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

import scala.{::, None, Option}

import cats.data.NonEmptyList
import cats.implicits._

import quasar.{IdStatus, ScalarStage, ScalarStages}
import quasar.api.ColumnType
import quasar.common.CPathField

object MaskedColumns {
  def apply(ss: ScalarStages): Option[(NonEmptyList[Ident], ScalarStages)] =
    ss match {
      case ScalarStages(IdStatus.ExcludeId, ScalarStage.Mask(m) :: rest) =>
        for {
          paths <- m.keySet.toList.toNel

          fields0 <- paths.traverse(_.head collect { case CPathField(f) => Ident(f) })
          fields = fields0.distinct

          eliminated = m forall {
            case (p, t) => p.tail.nodes.isEmpty && t === ColumnType.Top
          }
        } yield (fields, if (eliminated) ss.copy(stages = rest) else ss)

      case _ => None
    }
}
