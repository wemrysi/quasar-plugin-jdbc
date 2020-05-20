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

package quasar.plugin.avalanche

import scala.StringContext

import java.lang.String

import quasar.plugin.jdbc._

object AvalancheHygiene extends Hygiene {
  final case class HygienicIdent(asIdent: Ident) extends Hygienic {
	  /** @see https://docs.actian.com/avalanche/index.html#page/SQLLanguage%2FRegular_and_Delimited_Identifiers.htm%23ww414482 */
    def forSql: String = {
		  val escaped = asIdent.asString.replace("\"", "\"\"")
		  s""""$escaped""""
    }
  }

  def hygienicIdent(ident: Ident): HygienicIdent =
    HygienicIdent(ident)
}
