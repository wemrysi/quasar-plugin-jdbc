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

import scala.StringContext
import java.lang.String

import cats.{Order, Show}
import cats.instances.string._

final case class TableType(name: String) extends scala.AnyVal

object TableType {
  implicit val tableTypeOrder: Order[TableType] =
    Order.by(_.name.toLowerCase)

  implicit val tableTypeShow: Show[TableType] =
    Show.show(tt => s"TableType($tt)")
}
