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

package quasar.plugin.jdbc

import java.sql.Connection

import cats.{~>, Defer, Monad}
import cats.effect.Resource
import cats.syntax.functor._

import doobie._

object implicits {
  implicit final class TransactorOps[F[_]](val xa: Transactor[F]) extends scala.AnyVal {
    /** A natural transformation that runs a `ConnectionIO` using the provided `Connection`. */
    def runWith(c: Connection)(implicit F: Monad[F]): ConnectionIO ~> F =
      λ[ConnectionIO ~> F](_.foldMap(xa.interpret).run(c))

    /** A connection that uses the `Strategy` provided by the `Transactor`. */
    def strategicConnection(implicit F0: Monad[F], F1: Defer[F]): Resource[F, Connection] =
      xa.connect(xa.kernel).flatMap(c => xa.strategy.resource.mapK(runWith(c)).as(c))
  }
}
