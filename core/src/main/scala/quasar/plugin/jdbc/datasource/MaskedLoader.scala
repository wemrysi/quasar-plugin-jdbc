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

import scala.{None, Option, Some}

import cats.Applicative
import cats.data.Kleisli
import cats.effect.Resource

import quasar.ScalarStages
import quasar.connector.QueryResult

object MaskedLoader {
  /** Transforms a `MaskedLoader` into a `JdbcLoader` by extracting a `ColumnSelection`
    * from an initial `Mask` stage.
    *
    * @param hygiene a means of obtaining hygienic identifiers
    * @param loader the loader to transform
    */
  def apply[F[_]: Applicative](
      hygiene: Hygiene)(
      loader: MaskedLoader[F, hygiene.HygienicIdent])
      : JdbcLoader[F, hygiene.HygienicIdent] =
    loader.transform(k => Kleisli[Resource[F, ?], (hygiene.HygienicIdent, Option[hygiene.HygienicIdent], ScalarStages), QueryResult[F]] {
      case (table, schema, stages) =>
        MaskedColumns(stages) match {
          case Some((cols, moreStages)) =>
            val explicit = ColumnSelection.Explicit(cols.map(hygiene.hygienicIdent(_)))
            k((table, schema, explicit)).map(QueryResult.stages[F].set(moreStages))

          case None =>
            k((table, schema, ColumnSelection.All)).map(QueryResult.stages[F].set(stages))
        }
    })
}
