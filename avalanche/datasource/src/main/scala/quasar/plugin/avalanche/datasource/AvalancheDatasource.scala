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

import quasar.plugin.avalanche.AvalancheHygiene

import quasar.plugin.jdbc._
import quasar.plugin.jdbc.datasource._

import scala.Int
import java.lang.Throwable

import cats.Defer
import cats.data.NonEmptyList
import cats.effect.Bracket

import doobie._
import doobie.implicits._

import quasar.connector.MonadResourceErr
import quasar.connector.datasource.{LightweightDatasourceModule, Loader}

import org.slf4s.Logger

object AvalancheDatasource {
  val DefaultResultChunkSize: Int = 2048

  def apply[F[_]: Bracket[?[_], Throwable]: Defer: MonadResourceErr](
      xa: Transactor[F],
      discovery: JdbcDiscovery,
      log: Logger)
      : LightweightDatasourceModule.DS[F] = {

    val loader =
      (JdbcLoader(xa, discovery, AvalancheHygiene) _)
        .compose(MaskedLoader[ConnectionIO](AvalancheHygiene))
        .apply(AvalancheLoader(discovery, DefaultResultChunkSize))

    JdbcDatasource(
      xa,
      discovery,
      AvalancheDatasourceModule.kind,
      NonEmptyList.one(Loader.Batch(loader)))
  }
}
