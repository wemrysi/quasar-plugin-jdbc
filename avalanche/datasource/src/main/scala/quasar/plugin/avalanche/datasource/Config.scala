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

import scala.{Int, Option, StringContext}
import scala.concurrent.duration._
import scala.util.{Either, Left, Right}

import java.lang.String
import java.net.URI

import argonaut._, Argonaut._

import cats.implicits._

import quasar.plugin.jdbc.JdbcConfig

// TODO: enumerate more db properties? some may not be desirable so might not want to allow full URI control
//       like we don't want the end-user to be able to enable select loops if we aren't ready to handle them,
//       etc
// TODO: are there any properties we want to override/specify?
//
// Props to expose
//
// user: osUser
// password: osPassword
// role:
// group:
// dbms_user: dbmsUser
// dbms_password: dbmsPassword
// encryption:
// vnode_usage: ???
// timezone: ???
//
// @see https://docs.actian.com/avalanche/index.html#page/Connectivity%2FJDBC_Driver_Properties.htm%23ww243885
//
final case class Config(
    connectionUri: URI,
    maxConnections: Option[Int])
    extends JdbcConfig {

  import Config._

  // TODO: try and preserve non-sensitive parts of the query part?
  def sanitized: Config = {
    val sanitizedUri =
      new URI(
        connectionUri.getScheme,
        null,
        connectionUri.getHost,
        connectionUri.getPort,
        connectionUri.getPath,
        null,
        null)

    Config(sanitizedUri, maxConnections)
  }

  val connectionValidationTimeout =
    DefaultConnectionValidationTimeout

  def maxConcurrentConnections =
    maxConnections getOrElse DefaultMaxConcurrentConnections
}


object Config {
  val DefaultConnectionValidationTimeout: FiniteDuration = 5.seconds
  val DefaultMaxConcurrentConnections: Int = 8

  implicit val configCodecJson: CodecJson[Config] =
    CodecJson(
      config =>
        ("connectionUri" := config.connectionUri.toString) ->:
        ("maxConcurrentConnections" :=? config.maxConnections) ->?:
        jEmptyObject,

      cursor => {
        val uriCursor = cursor --\ "connectionUri"

        for {
          uriStr <- uriCursor.as[String]

          uri <- Either.catchNonFatal(new URI(uriStr)) match {
            case Left(t) => DecodeResult.fail(s"Malformed connection URI: ${t.getMessage}", uriCursor.history)
            case Right(u) => DecodeResult.ok(u)
          }

          maxConn <- (cursor --\ "maxConcurrentConnections").as[Option[Int]]
        } yield Config(uri, maxConn)
      })
}
