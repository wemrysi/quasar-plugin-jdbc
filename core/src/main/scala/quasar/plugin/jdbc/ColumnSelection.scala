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

import scala.{None, Nothing, Product, Serializable, Some}

import cats._
import cats.data.NonEmptyList
import cats.implicits._

sealed trait ColumnSelection[+I] extends Product with Serializable

object ColumnSelection {
  case object All extends ColumnSelection[Nothing]
  final case class Explicit[I](identifiers: NonEmptyList[I]) extends ColumnSelection[I]

  implicit val columnSelectionSemigroupK: SemigroupK[ColumnSelection] =
    new SemigroupK[ColumnSelection] {
      def combineK[A](x: ColumnSelection[A], y: ColumnSelection[A]) =
        (x, y) match {
          case (All, _) => All
          case (_, All) => All
          case (Explicit(xs), Explicit(ys)) => Explicit(xs ::: ys)
        }
    }

  implicit val columnSelectionTraverse: Traverse[ColumnSelection] =
    new Traverse[ColumnSelection] {
      def traverse[G[_]: Applicative, A, B](fa: ColumnSelection[A])(f: A => G[B]): G[ColumnSelection[B]] =
        fa match {
          case All => (All: ColumnSelection[B]).pure[G]
          case Explicit(as) => as.traverse(f).map(Explicit(_))
        }

      def foldLeft[A, B](fa: ColumnSelection[A], b: B)(f: (B, A) => B): B =
        fa match {
          case All => b
          case Explicit(as) => as.foldLeft(b)(f)
        }

      def foldRight[A, B](fa: ColumnSelection[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        fa match {
          case All => lb
          case Explicit(as) => as.foldRight(lb)(f)
        }
    }

  implicit def columnSelectionEq[I: Eq]: Eq[ColumnSelection[I]] =
    Eq by {
      case All => None
      case Explicit(ns) => Some(ns)
    }

  implicit def columnSelectionShow[I: Show]: Show[ColumnSelection[I]] =
    Show show {
      case All =>
        "ColumnSelection(*)"

      case Explicit(ns) =>
        ns.map(_.show).toList.mkString("ColumnSelection(", ", ", ")")
    }
}
