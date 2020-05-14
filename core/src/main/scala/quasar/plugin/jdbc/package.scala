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

package quasar.plugin

import scala.{Option, Some}
import scala.util.{Either, Left, Right}

import java.lang.String

import quasar.api.resource.{/:, ResourcePath}

package object jdbc {

  /** A reference to a database object. */
  type DboRef = Either[Ident, (Ident, Ident)]

  type ColumnName = Ident
  def ColumnName(name: String): ColumnName = Ident(name)

  type SchemaName = Ident
  def SchemaName(name: String): SchemaName = Ident(name)

  type TableName = Ident
  def TableName(name: String): TableName = Ident(name)

  // The name of a vendor-specific database type
  type VendorType = String

  val Redacted: String = "<REDACTED>"

  def resourcePathRef(p: ResourcePath): Option[DboRef] =
    Some(p) collect {
      case fst /: ResourcePath.Root =>
        Left(Ident(fst))

      case fst /: snd /: ResourcePath.Root =>
        Right((Ident(fst), Ident(snd)))
    }
}
