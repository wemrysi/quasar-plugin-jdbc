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

package quasar.plugin.jdbc.datasource

import quasar.plugin.jdbc._
import quasar.plugin.jdbc.implicits._

import scala.{Boolean, Char, Int, Nil, None, Option, Some}
import scala.collection.immutable.Set
import scala.util.{Either, Left, Right}

import java.lang.{String, Throwable}

import cats.{~>, Defer}
import cats.data.{Ior, NonEmptySet}
import cats.effect.{Bracket, Resource}
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.{Pull, Stream}

import io.chrisdavenport.log4cats.Logger

import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType => RPT}
import quasar.connector.datasource.LightweightDatasourceModule

import shims.equalToCats

trait JdbcDatasource[F[_]] extends LightweightDatasourceModule.DS[F] {
  protected def xa: Transactor[F]
  protected def log: Logger[F]

  implicit protected def BracketF: Bracket[F, Throwable]
  implicit protected def DeferF: Defer[F]

  /** The set of table types that should be discoverable.
    *
    * A table having a type not in this set will not be considered a resource
    * by `pathIsResource` nor appear in results of `prefixedChildPaths`.
    *
    * @see https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getTableTypes--
    */
  protected def discoverableTableTypes: ConnectionIO[NonEmptySet[TableType]]

  /** Returns whether a table having the specified name and schema exists in the database.
    *
    * If `schema` is `None`, the table is expected to not have a schema.
    */
  protected def tableExists(table: TableName, schema: Option[SchemaName]): ConnectionIO[Boolean] =
    Stream.eval(tableSelector(schema.fold(Ior.right[SchemaName, TableName](table))(Ior.both(_, table))))
      .flatMap(tables)
      .exists(m => m.schema === schema && m.table === table)
      .compile
      .lastOrError

  ////

  def pathIsResource(path: ResourcePath): Resource[F, Boolean] =
    resourcePathRef(path).fold(false.pure[Resource[F, ?]]) {
      case Left(table) =>
        Resource.liftF(tableExists(table, None).transact(xa))

      case Right((schema, table)) =>
        Resource.liftF(tableExists(table, Some(schema)).transact(xa))
    }

  def prefixedChildPaths(prefixPath: ResourcePath): Resource[F, Option[Stream[F, (ResourceName, RPT.Physical)]]] = {
    type Out[X[_]] = Stream[X, (ResourceName, RPT.Physical)]

    if (prefixPath === ResourcePath.Root)
      topLevel
        .map(_.fold(
          s => (ResourceName(s.asString), RPT.prefix),
          t => (ResourceName(t.asString), RPT.leafResource)))
        .transact(xa)
        .some
        .pure[Resource[F, ?]]
    else
      resourcePathRef(prefixPath).fold((None: Option[Out[F]]).pure[Resource[F, ?]]) {
        case Right((schema, table)) =>
          Resource liftF {
            tableExists(table, Some(schema))
              .map(p => if (p) Some(Stream.empty: Out[F]) else None)
              .transact(xa)
          }

        case Left(ident) =>
          def paths =
            Stream.eval(tableSelector(Ior.left(ident)))
              .flatMap(tables)
              .map(m => (ResourceName(m.table.asString), RPT.leafResource))
              .pull.peek1
              .flatMap(t => Pull.output1(t.map(_._2)))
              .stream

          for {
            c <- xa.strategicConnection

            isTable <- Resource.liftF(xa.runWith(c).apply(tableExists(ident, None)))

            opt <- if (isTable)
              Resource.pure[F, Option[Out[ConnectionIO]]](Some(Stream.empty))
            else
              paths.compile.resource.lastOrError.mapK(xa.runWith(c))
          } yield opt.map(_.translate(xa.runWith(c)))
      }
  }

  ////

  private case class TableMeta(table: TableName, schema: Option[SchemaName])

  private object TableMeta {
    /** Only usable with the ResultSet returned from `DatabaseMetaData#getTables`
      *
      *  1. TABLE_CAT String => table catalog (may be null)
      *  2. TABLE_SCHEM String => table schema (may be null)
      *  3. TABLE_NAME String => table name
      *  4. TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
      *  5. REMARKS String => explanatory comment on the table
      *  6. TYPE_CAT String => the types catalog (may be null)
      *  7. TYPE_SCHEM String => the types schema (may be null)
      *  8. TYPE_NAME String => type name (may be null)
      *  9. SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
      * 10. REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null)
      */
    implicit val tableMetaRead: Read[TableMeta] =
      new Read[TableMeta](Nil, (rs, _) =>
        TableMeta(
          TableName(rs.getString(3)),
          Option(rs.getString(2)).map(SchemaName)))
  }

  private type TableSelector = (String, String)

  private val AllTablesSelector: TableSelector = (null, "%")

  // Characters considered pattern placeholders by `DatabaseMetaData#getTables`
  private val Wildcards: Set[Char] = Set('_', '%')

  private def tables(selector: TableSelector): Stream[ConnectionIO, TableMeta] = {
    val (schemaPattern, tablePattern) = selector

    Stream.force(for {
      catalog <- HC.getCatalog
      types <- discoverableTableTypes
      typeMask = types.map(_.name).toSortedSet.toArray
      rs <- HC.getMetaData(FDMD.getTables(catalog, schemaPattern, tablePattern, typeMask))
      ts = HRS.stream[TableMeta](JdbcDatasource.MetaChunkSize)
    } yield ts.translate(λ[ResultSetIO ~> ConnectionIO](FC.embed(rs, _))))
  }

  private def tableSelector(ref: SchemaName Ior TableName): ConnectionIO[TableSelector] = {
    def escapeForSearch(escapeString: String, patternLiteral: String): String =
      Wildcards.foldLeft(patternLiteral) {
        case (lit, wc) => lit.replace(wc.toString, escapeString + wc)
      }

    HC.getMetaData(FDMD.getSearchStringEscape) map { escape =>
      ref match {
        // All tables in the schema
        case Ior.Left(schema) =>
          (escapeForSearch(escape, schema.asString), "%")

        // The table having no schema
        case Ior.Right(table) =>
          ("", escapeForSearch(escape, table.asString))

        // The table in the specified schema
        case Ior.Both(schema, table) =>
          (escapeForSearch(escape, schema.asString), escapeForSearch(escape, table.asString))
      }
    }
  }

  private def topLevel: Stream[ConnectionIO, Either[SchemaName, TableName]] =
    tables(AllTablesSelector)
      .scan((Set.empty[SchemaName], None: Option[Either[SchemaName, TableName]])) {
        case ((seen, _), TableMeta(_, Some(schema))) =>
          if (seen(schema))
            (seen, None)
          else
            (seen + schema, Some(Left(schema)))

        case ((seen, _), TableMeta(table, None)) =>
          (seen, Some(Right(table)))
      }
      .map(_._2)
      .unNone
}

object JdbcDatasource {
  /** The chunk size used for metadata streams. */
  val MetaChunkSize: Int = 1024
}
