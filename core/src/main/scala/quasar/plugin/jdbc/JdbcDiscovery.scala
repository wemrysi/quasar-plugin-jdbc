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

import scala.{Boolean, Char, Int, Nil, None, Option, Some}
import scala.collection.immutable.Set
import scala.util.{Either, Left, Right}

import java.lang.String

import cats.~>
import cats.data.{Ior, NonEmptySet}
import cats.implicits._

import doobie._
import doobie.enum.{ColumnNullable, JdbcType}
import doobie.implicits._

import fs2.Stream

/** Provides a means of discovering schemas and tables in a database. */
final class JdbcDiscovery private (discoverableTableTypes: Option[ConnectionIO[NonEmptySet[TableType]]]) {
  import JdbcDiscovery.{ColumnMeta, TableMeta}

  /** Returns metadata for all discoverable tables. */
  val allTables: Stream[ConnectionIO, TableMeta] =
    selectedTables((null, "%"))

  /** Returns metadata about all of the columns in the specified table.
    *
    * If `schema` is `None`, the table is expected to not have a schema.
    */
  def tableColumns(table: TableName, schema: Option[SchemaName]): Stream[ConnectionIO, ColumnMeta] = {
    val selector = schema.fold(Ior.right[SchemaName, TableName](table))(Ior.both(_, table))
    Stream.force(tableSelector(selector).map(selectedColumns))
  }

  /** Returns whether a table having the specified name and schema exists in the database.
    *
    * If `schema` is `None`, the table is expected to not have a schema.
    */
  def tableExists(table: TableName, schema: Option[SchemaName]): ConnectionIO[Boolean] =
    tables(schema.fold(Ior.right[SchemaName, TableName](table))(Ior.both(_, table)))
      .exists(m => m.schema === schema && m.table === table)
      .compile
      .lastOrError

  /** Returns metadata for the tables matched by the given selector.
    *
    * `Left(schema)`: selects all tables in the schema
    * `Right(table)`: selects the table having no schema
    * `Both(schema, table)` selects the table in the schema
    */
  def tables(selector: SchemaName Ior TableName): Stream[ConnectionIO, TableMeta] =
    Stream.force(tableSelector(selector).map(selectedTables))

  /** Returns a stream consisting of "top-level" objects in the database: schemas
    * and tables without a schema.
    */
  def topLevel: Stream[ConnectionIO, Either[SchemaName, TableName]] =
    allTables
      .scan((Set.empty[SchemaName], None: Option[Either[SchemaName, TableName]])) {
        case ((seen, _), TableMeta(_, Some(schema), _)) =>
          if (seen(schema))
            (seen, None)
          else
            (seen + schema, Some(Left(schema)))

        case ((seen, _), TableMeta(table, None, _)) =>
          (seen, Some(Right(table)))
      }
      .map(_._2)
      .unNone

  ////

  private type TableSelector = (String, String)

  // Characters considered pattern placeholders by `DatabaseMetaData#getTables`
  private val Wildcards: Set[Char] = Set('_', '%')

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
  private implicit val tableMetaRead: Read[TableMeta] =
    new Read[TableMeta](Nil, (rs, _) =>
      TableMeta(
        TableName(rs.getString(3)),
        Option(rs.getString(2)).map(SchemaName),
        TableType(rs.getString(4))))

  /** Only usable with the ResultSet returned from `DatabaseMetaData#getColumns`
    *
    * 1. TABLE_CAT String => table catalog (may be null)
    * 2. TABLE_SCHEM String => table schema (may be null)
    * 3. TABLE_NAME String => table name
    * 4. COLUMN_NAME String => column name
    * 5. DATA_TYPE int => SQL type from java.sql.Types
    * 6. TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
    * 7. COLUMN_SIZE int => column size.
    * 8. BUFFER_LENGTH is not used.
    * 9. DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
    * 10. NUM_PREC_RADIX int => Radix (typically either 10 or 2)
    * 11. NULLABLE int => is NULL allowed.
    *     columnNoNulls - might not allow NULL values
    *     columnNullable - definitely allows NULL values
    *     columnNullableUnknown - nullability unknown
    * 12. REMARKS String => comment describing column (may be null)
    * 13. COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null)
    * 14. SQL_DATA_TYPE int => unused
    * 15. SQL_DATETIME_SUB int => unused
    * 16. CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
    * 17. ORDINAL_POSITION int => index of column in table (starting at 1)
    * 18. IS_NULLABLE String => ISO rules are used to determine the nullability for a column.
    *     YES --- if the column can include NULLs
    *     NO --- if the column cannot include NULLs
    *     empty string --- if the nullability for the column is unknown
    * 19. SCOPE_CATALOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
    * 20. SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
    * 21. SCOPE_TABLE String => table name that this the scope of a reference attribute (null if the DATA_TYPE isn't REF)
    * 22. SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)
    * 23. IS_AUTOINCREMENT String => Indicates whether this column is auto incremented
    *     YES --- if the column is auto incremented
    *     NO --- if the column is not auto incremented
    *     empty string --- if it cannot be determined whether the column is auto incremented
    * 24. IS_GENERATEDCOLUMN String => Indicates whether this is a generated column
    *    YES --- if this a generated column
    *    NO --- if this not a generated column
    *    empty string --- if it cannot be determined whether this is a generated column
    */
  private implicit val columnMetaRead: Read[ColumnMeta] =
    new Read[ColumnMeta](Nil, (rs, _) =>
      ColumnMeta(
        ColumnName(rs.getString(4)),
        JdbcType.fromInt(rs.getInt(5)),
        rs.getString(6),
        ColumnNullable.fromInt(rs.getInt(11)) getOrElse ColumnNullable.NullableUnknown))

  private def selectedColumns(selector: TableSelector): Stream[ConnectionIO, ColumnMeta] = {
    val (schemaPattern, tablePattern) = selector

    Stream.force(for {
      catalog <- HC.getCatalog
      rs <- HC.getMetaData(FDMD.getColumns(catalog, schemaPattern, tablePattern, "%"))
      ts = HRS.stream[ColumnMeta](JdbcDiscovery.MetaChunkSize)
    } yield ts.translate(λ[ResultSetIO ~> ConnectionIO](FC.embed(rs, _))))
  }

  private def selectedTables(selector: TableSelector): Stream[ConnectionIO, TableMeta] = {
    val (schemaPattern, tablePattern) = selector

    Stream.force(for {
      catalog <- HC.getCatalog
      types <- discoverableTableTypes.sequence
      typeMask = types.map(_.map(_.name).toSortedSet.toArray)
      rs <- HC.getMetaData(FDMD.getTables(catalog, schemaPattern, tablePattern, typeMask.orNull))
      ts = HRS.stream[TableMeta](JdbcDiscovery.MetaChunkSize)
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
}

object JdbcDiscovery {
  final case class TableMeta(
      table: TableName,
      schema: Option[SchemaName],
      tableType: TableType)

  final case class ColumnMeta(
      name: ColumnName,
      jdbcType: JdbcType,
      vendorType: VendorType,
      columnNullable: ColumnNullable)

  /** The chunk size used for metadata streams. */
  val MetaChunkSize: Int = 1024

  /** Provides a means of discovering schemas and tables in a database.
    *
    * @param discoverableTableTypes the set of table types, from `DatabaseMetaData#getTableTypes`,
    *                               that should be discoverable. `None` means all types are discoverable.
    *
    * @see https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getTableTypes--
    */
  def apply(discoverableTableTypes: Option[ConnectionIO[NonEmptySet[TableType]]])
      : JdbcDiscovery =
    new JdbcDiscovery(discoverableTableTypes)
}
