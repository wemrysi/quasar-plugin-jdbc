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

import scala._, Predef._
import scala.concurrent.duration._

import java.lang.String

import argonaut._, Argonaut._

import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits._

import quasar.plugin.jdbc.Redacted

/** Avalanche connection configuration.
  *
  * @see https://docs.actian.com/avalanche/index.html#page/Connectivity%2FData_Source_Properties.htm%23
  */
final case class ConnectionConfig (
    serverName: String,
    databaseName: String,
    maxConcurrency: Option[Int],
    maxLifetime: Option[FiniteDuration],
    properties: Map[String, String]) {

  import ConnectionConfig._

  def dataSourceProperites: Map[String, String] =
    properties
      .updated(ServerNameProp, serverName)
      .updated(DatabaseNameProp, databaseName)

  def sanitized: ConnectionConfig = {
    val sanitizedProps =
      SensitiveProps.foldLeft(properties) {
        case (ps, p) if ps.contains(p) =>
          // Roles may include a role password via 'name|password',
          // so we need to redact the password, if present
          if (p == RoleNameProp) {
            val parts = ps(p).split('|')
            ps.updated(p, if (parts.length > 1) s"${parts(0)}|$Redacted" else parts(0))
          } else {
            ps.updated(p, Redacted)
          }

        case (ps, _) => ps
      }

    copy(properties = sanitizedProps)
  }

  def validated: ValidatedNel[String, ConnectionConfig] = {
    val invalidProps = properties.keySet -- ConfigurableProps

    invalidProps.toList.toNel.toInvalid(this) leftMap { ps =>
      NonEmptyList.one(ps.toList.mkString("Unsupported properties: ", ", ", ""))
    }
  }
}

object ConnectionConfig {
  val ServerNameProp: String = "serverName"
  val DatabaseNameProp: String = "databaseName"
  val RoleNameProp: String = "roleName"

  /** Properties having values that should never be displayed. */
  val SensitiveProps: Set[String] =
    Set("dbmsPassword", "password", RoleNameProp)

  /** The configurable Avalanche DataSource properties. */
  val ConfigurableProps: Set[String] =
    Set(
      "compression",
      "encryption",
      "dbmsUser",
      "groupName",
      "user",
      "vnodeUsage"
    ) ++ SensitiveProps

  implicit val connectionConfigCodecJson: CodecJson[ConnectionConfig] =
    CodecJson(
      cc => {
        val noProps =
          ("serverName" := cc.serverName) ->:
          ("databaseName" := cc.databaseName) ->:
          ("maxConcurrency" :=? cc.maxConcurrency) ->?:
          ("maxLifetimeSecs" :=? cc.maxLifetime.map(_.toSeconds)) ->?:
          jEmptyObject

        if (cc.properties.isEmpty)
          noProps
        else
          ("properties" := cc.properties) ->: noProps
      },

      cursor => for {
        serverName <- (cursor --\ "serverName").as[String]
        databaseName <- (cursor --\ "databaseName").as[String]
        maxConcurrency <- (cursor --\ "maxConcurrency").as[Option[Int]]

        maxLifetimeSecs <- (cursor --\ "maxLifetimeSecs").as[Option[Int]]
        maxLifetime = maxLifetimeSecs.map(_.seconds)

        maybeProps <- (cursor --\ "properties").as[Option[Map[String, Option[String]]]]
        props = maybeProps.fold(Map.empty[String, String])(_.flattenOption)
      } yield ConnectionConfig(serverName, databaseName, maxConcurrency, maxLifetime, props))
}
