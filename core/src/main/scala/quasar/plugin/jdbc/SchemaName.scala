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

import java.lang.String

import cats.{Order, Show}

final case class SchemaName(asIdent: Ident) {
  def asString: String = asIdent.asString
}

object SchemaName {
  def fromString(name: String): SchemaName =
    SchemaName(Ident(name))

  implicit val schemaNameOrder: Order[SchemaName] =
    Order.by(_.asIdent)

  implicit val schemaNameShow: Show[SchemaName] =
    Show.show(s => "SchemaName(" + s.asString + ")")
}
