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

// TODO: Doobie query logging
abstract class JdbcDatasource[F[_]: Bracket[?[_], Throwable]: Defer](
    xa: Transactor[F],
    log: Logger[F])
    extends LightweightDatasourceModule.DS[F] {

  /** A SQL identifier that has been escaped and quoted as necessary for
    * literal substitution into SQL query string.
    */
  type HygienicIdent <: Hygienic

  /** The set of table types that should be discoverable.
    *
    * A table having a type not in this set will not be considered a resource
    * by `pathIsResource` nor appear in results of `prefixedChildPaths`.
    *
    * @see https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getTableTypes--
    */
  def discoverableTableTypes: NonEmptySet[TableType]

  /** Returns a hygienic version of the given identifier. */
  def hygienicIdent(ident: Ident): HygienicIdent

  ////

  def pathIsResource(path: ResourcePath): Resource[F, Boolean] =
    resourcePathRef(path).fold(false.pure[Resource[F, ?]]) {
      case Left(table) =>
        Resource.liftF(tableExists(None, TableName(table)).transact(xa))

      case Right((schema, table)) =>
        Resource.liftF(tableExists(Some(SchemaName(schema)), TableName(table)).transact(xa))
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
            tableExists(Some(SchemaName(schema)), TableName(table))
              .map(p => if (p) Some(Stream.empty: Out[F]) else None)
              .transact(xa)
          }

        case Left(ident) =>
          def paths =
            Stream.force(tableSelector(Ior.left(SchemaName(ident))).map(tables))
              .map(m => (ResourceName(m.table.asString), RPT.leafResource))
              .pull.peek1
              .flatMap(t => Pull.output1(t.map(_._2)))
              .stream

          for {
            c <- xa.connect(xa.kernel)

            isTable <- Resource.liftF(runCIO(c)(tableExists(None, TableName(ident))))

            opt <- if (isTable)
              Resource.pure[F, Option[Out[ConnectionIO]]](Some(Stream.empty))
            else
              paths.compile.resource.lastOrError.mapK(runCIO(c))
          } yield opt.map(_.translate(runCIO(c)))
      }
  }

  ////

  private case class TableMeta(schema: Option[SchemaName], table: TableName)

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
          Option(rs.getString(2)).map(SchemaName.fromString(_)),
          TableName.fromString(rs.getString(3))))
  }

  private type TableSelector = (String, String)

  private val AllTablesSelector: TableSelector = (null, "%")

  // Characters considered pattern placeholders by `DatabaseMetaData#getTables`
  private val Wildcards: Set[Char] = Set('_', '%')

  private def runCIO(c: java.sql.Connection): ConnectionIO ~> F =
    λ[ConnectionIO ~> F](_.foldMap(xa.interpret).run(c))

  private def tableExists(schema: Option[SchemaName], table: TableName): ConnectionIO[Boolean] =
    Stream.eval(tableSelector(schema.fold(Ior.right[SchemaName, TableName](table))(Ior.both(_, table))))
      .flatMap(tables)
      .exists(m => m.schema === schema && m.table === table)
      .compile
      .lastOrError

  private def tables(selector: TableSelector): Stream[ConnectionIO, TableMeta] = {
    val (schemaPattern, tablePattern) = selector

    Stream.force(for {
      catalog <- HC.getCatalog
      theseTypes = discoverableTableTypes.map(_.name).toSortedSet.toArray
      rs <- HC.getMetaData(FDMD.getTables(catalog, schemaPattern, tablePattern, theseTypes))
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
        case ((seen, _), TableMeta(Some(schema), _)) =>
          if (seen(schema))
            (seen, None)
          else
            (seen + schema, Some(Left(schema)))

        case ((seen, _), TableMeta(None, table)) =>
          (seen, Some(Right(table)))
      }
      .map(_._2)
      .unNone
}

object JdbcDatasource {
  /** The chunk size used for metadata streams. */
  val MetaChunkSize: Int = 1024
}
