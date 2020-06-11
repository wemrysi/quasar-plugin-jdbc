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

import doobie.util.log._

import org.slf4s.Logger

object Slf4sLogHandler {
  def apply(log: Logger): LogHandler =
    LogHandler {
      case Success(q, _, e, p) =>
        log.debug(s"[SQL Execution Succeeded] `$q` in ${(e + p).toMillis} ms (${e.toMillis} ms exec, ${p.toMillis} ms proc)")

      case ExecFailure(q, _, e, t) =>
        log.error(s"[SQL Execution Failed] `$q` after ${e.toMillis} ms, cause: $t", t)

      case ProcessingFailure(q, _, e, p, t) =>
        log.error(s"[SQL ResultSet Processing Failed] `$q` after ${(e + p).toMillis} ms (${e.toMillis} ms exec, ${p.toMillis} ms proc (failed)), cause: $t", t)
    }
}
