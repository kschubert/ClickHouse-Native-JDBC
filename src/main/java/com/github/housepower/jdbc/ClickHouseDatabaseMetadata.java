package com.github.housepower.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collector;

import com.github.housepower.jdbc.data.Block;
import com.github.housepower.jdbc.data.Column;
import com.github.housepower.jdbc.data.DataTypeFactory;
import com.github.housepower.jdbc.data.IDataType;
import com.github.housepower.jdbc.data.type.DataTypeInt32;
import com.github.housepower.jdbc.data.type.DataTypeInt8;
import com.github.housepower.jdbc.data.type.DataTypeString;
import com.github.housepower.jdbc.misc.CheckedIterator;
import com.github.housepower.jdbc.protocol.DataResponse;
import com.github.housepower.jdbc.statement.ClickHouseStatement;
import com.github.housepower.jdbc.util.ClickHouseVersionNumberUtil;

public class ClickHouseDatabaseMetadata implements DatabaseMetaData {

    public static final class OneBlockIterator implements CheckedIterator<DataResponse, SQLException> {
        private final Block header;
        boolean consumed;

        public OneBlockIterator(Block header) {
            this.header = header;
        }

        @Override
        public DataResponse next() throws SQLException {
            consumed = true;
            return new DataResponse("unused", header);
        }

        @Override
        public boolean hasNext() throws SQLException {
            return !consumed;
        }
    }

    static final String DEFAULT_CAT = "default";

//    private static final Logger log = LoggerFactory.getLogger(ClickHouseDatabaseMetadata.class);

    private final String url;
    private final ClickHouseConnection connection;

    public ClickHouseDatabaseMetadata(String url, ClickHouseConnection connection) {
        this.url = url;
        this.connection = connection;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return true;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return url;
    }

    @Override
    public String getUserName() throws SQLException {
        return null;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "ClickHouse";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return connection.getServerVersion();
    }

    @Override
    public String getDriverName() throws SQLException {
        return "com.github.housepower.clickhouse-jdbc";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        String driverVersion = getClass().getPackage().getImplementationVersion();
        return driverVersion != null ? driverVersion : "0.1";
    }

    @Override
    public int getDriverMajorVersion() {
        String v;
        try {
            v = getDriverVersion();
        } catch (SQLException sqle) {
//            log.warn("Error determining driver major version", sqle);
            return 0;
        }
        return ClickHouseVersionNumberUtil.getMajorVersion(v);
    }

    @Override
    public int getDriverMinorVersion() {
        String v;
        try {
            v = getDriverVersion();
        } catch (SQLException sqle) {
//            log.warn("Error determining driver minor version", sqle);
            return 0;
        }
        return ClickHouseVersionNumberUtil.getMinorVersion(v);
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "`";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "GLOBAL,ARRAY";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "database";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return level == Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    private ResultSet request(String sql) throws SQLException {
        Statement statement = connection.createStatement();
        return statement.executeQuery(sql);
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        Block header = setupHeader(0, new Column[] {
                new Column("PROCEDURE_CAT", new DataTypeString()),
                new Column("PROCEDURE_SCHEM", new DataTypeString()),
                new Column("PROCEDURE_NAME", new DataTypeString()),
                new Column("RES_1", new DataTypeString()),
                new Column("RES_2", new DataTypeString()),
                new Column("RES_3", new DataTypeString()),
                new Column("REMARKS", new DataTypeString()),
                new Column("PROCEDURE_TYPE", new DataTypeInt8("UInt8")),
                new Column("SPECIFIC_NAME", new DataTypeString()) });
        return new ClickHouseResultSet(header, new OneBlockIterator(header), new ClickHouseStatement(connection));
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        Block header = setupHeader(0, new Column[] {
                new Column("1", new DataTypeInt32("UInt32")),
                new Column("2", new DataTypeInt32("UInt32")),
                new Column("3", new DataTypeInt32("UInt32")),
                new Column("4", new DataTypeInt32("UInt32")),
                new Column("5", new DataTypeInt32("UInt32")),
                new Column("6", new DataTypeInt32("UInt32")),
                new Column("7", new DataTypeInt32("UInt32")),
                new Column("8", new DataTypeInt32("UInt32")),
                new Column("9", new DataTypeInt32("UInt32")),
                new Column("10", new DataTypeInt32("UInt32")),
                new Column("11", new DataTypeInt32("UInt32")),
                new Column("12", new DataTypeInt32("UInt32")),
                new Column("13", new DataTypeInt32("UInt32")),
                new Column("14", new DataTypeInt32("UInt32")),
                new Column("15", new DataTypeInt32("UInt32")),
                new Column("16", new DataTypeInt32("UInt32")),
                new Column("17", new DataTypeInt32("UInt32")),
                new Column("18", new DataTypeInt32("UInt32")),
                new Column("19", new DataTypeInt32("UInt32")),
                new Column("20", new DataTypeInt32("UInt32")) });
        return new ClickHouseResultSet(header, new OneBlockIterator(header), new ClickHouseStatement(connection));
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        /*
         * TABLE_CAT String => table catalog (may be null) TABLE_SCHEM String => table
         * schema (may be null) TABLE_NAME String => table name TABLE_TYPE String =>
         * table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE",
         * "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM". REMARKS String =>
         * explanatory comment on the table TYPE_CAT String => the types catalog (may be
         * null) TYPE_SCHEM String => the types schema (may be null) TYPE_NAME String =>
         * type name (may be null) SELF_REFERENCING_COL_NAME String => name of the
         * designated "identifier" column of a typed table (may be null) REF_GENERATION
         * String => specifies how values in SELF_REFERENCING_COL_NAME are created.
         * Values are "SYSTEM", "USER", "DERIVED". (may be null)
         */
        String sql = "select database, name, engine from system.tables where 1 = 1";
        if (schemaPattern != null) {
            sql += " and database like '" + schemaPattern + "'";
        }
        if (tableNamePattern != null) {
            sql += " and name like '" + tableNamePattern + "'";
        }
        sql += " order by database, name";
        ResultSet result = request(sql);

        List<String> typeList = types != null ? Arrays.asList(types) : null;
        List<List<Object>> rows = new ArrayList<>();
        while (result.next()) {
            List<Object> row = new ArrayList<>();
            row.add(DEFAULT_CAT);
            row.add(result.getString(1));
            row.add(result.getString(2));
            String targetType;
            String engine = result.getString(3).intern();
            if (engine == null) {
                targetType = "TABLE";
            } else if (engine.equals("View") || engine.equals("MaterializedView") || engine.equals("Merge")
                    || engine.equals("Distributed") || engine.equals("Null")) {
                targetType = "VIEW"; // some kind of view
            } else if (engine == "Set" || engine == "Join" || engine == "Buffer") {
                targetType = "OTHER"; // not a real table
            } else {
                targetType = "TABLE";
            }
            row.add(targetType);
            if (typeList == null || typeList.contains(targetType)) {
                rows.add(row);
            }
        }
        result.close();
        List<List<Object>> columnValues = rowStoredToColumnStored(rows);
        Object[] allNullsColumn = new Object[rows.size()];
        Column[] columns = new Column[] {
                new Column("TABLE_CAT", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("TABLE_SCHEM", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("TABLE_NAME", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("TABLE_TYPE", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("REMARKS", new DataTypeString(), allNullsColumn),
                new Column("TYPE_CAT", new DataTypeString(), allNullsColumn),
                new Column("TYPE_SCHEM", new DataTypeString(), allNullsColumn),
                new Column("TYPE_NAME", new DataTypeString(), allNullsColumn),
                new Column("SELF_REFERENCING_COL_NAME", new DataTypeString(), allNullsColumn),
                new Column("REF_GENERATION", new DataTypeString(), allNullsColumn) };

        Block header = setupHeader(rows.size(), columns);
        return new ClickHouseResultSet(header, new OneBlockIterator(header), new ClickHouseStatement(connection));
    }

    private static List<List<Object>> rowStoredToColumnStored(List<List<Object>> rows) {
        return rows.stream().collect(
                Collector.of(() -> new ArrayList<List<Object>>(), (List<List<Object>> accu, List<Object> row) -> {
                    ListIterator<List<Object>> columnIter = accu.listIterator();
                    row.forEach(value -> {
                        if (columnIter.hasNext()) {
                            columnIter.next().add(value);
                        } else {
                            ArrayList<Object> columnValues = new ArrayList<Object>();
                            columnIter.add(columnValues);
                            columnValues.add(value);
                        }
                    });
                }, (List<List<Object>> left, List<List<Object>> right) -> {
                    left.forEach(column -> column.addAll(right.remove(0)));
                    return left;
                }));
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        String sql = "select name as TABLE_SCHEM, '" + DEFAULT_CAT + "' as TABLE_CATALOG from system.databases";
        if (catalog != null) {
            sql += " where TABLE_CATALOG = '" + catalog + '\'';
        }
        if (schemaPattern != null) {
            if (catalog != null) {
                sql += " and ";
            } else {
                sql += " where ";
            }
            sql += "name LIKE '" + schemaPattern + '\'';
        }
        return request(sql);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        Block header = setupHeader(1,
                new Column[] { new Column("TABLE_CAT", new DataTypeString(), new Object[] { DEFAULT_CAT }) });
        return new ClickHouseResultSet(header, new OneBlockIterator(header), new ClickHouseStatement(connection));
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        Block header = setupHeader(3, new Column[] {
                new Column("TABLE_TYPE", new DataTypeString(), new Object[] { "TABLE", "VIEW", "OTHER" }) });
        return new ClickHouseResultSet(header, new OneBlockIterator(header), new ClickHouseStatement(connection));
    }

    private static void buildAndCondition(StringBuilder dest, List<String> conditions) {
        Iterator<String> iter = conditions.iterator();
        if (iter.hasNext()) {
            String entry = iter.next();
            dest.append(entry);
        }
        while (iter.hasNext()) {
            String entry = iter.next();
            dest.append(" AND ").append(entry);
        }
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        StringBuilder query;
        if (connection.getServerVersion().compareTo("1.1.54237") > 0) {
            query = new StringBuilder(
                    "SELECT database, table, name, type, default_kind as default_type, default_expression ");
        } else {
            query = new StringBuilder("SELECT database, table, name, type, default_type, default_expression ");
        }
        query.append("FROM system.columns ");
        List<String> predicates = new ArrayList<String>();
        if (schemaPattern != null) {
            predicates.add("database LIKE '" + schemaPattern + "' ");
        }
        if (tableNamePattern != null) {
            predicates.add("table LIKE '" + tableNamePattern + "' ");
        }
        if (columnNamePattern != null) {
            predicates.add("name LIKE '" + columnNamePattern + "' ");
        }
        if (!predicates.isEmpty()) {
            query.append(" WHERE ");
            buildAndCondition(query, predicates);
        }
        ResultSet descTable = request(query.toString());
        int colNum = 1;
        List<List<Object>> rows = new ArrayList<>();
        while (descTable.next()) {
            List<Object> row = new ArrayList<>();
            // catalog name
            row.add(DEFAULT_CAT);
            // database name
            row.add(descTable.getString("database"));
            // table name
            row.add(descTable.getString("table"));
            // column name
            IDataType columnInfo = DataTypeFactory.get(descTable.getString("type"), connection.serverInfo());
            row.add(descTable.getString("name"));
            // data type
            row.add(String.valueOf(columnInfo.sqlTypeId()));
            // type name
            row.add(columnInfo.name());
            // column size / precision
            row.add(columnInfo.precisionOrLength());
            // buffer length
            row.add("0");
            // decimal digits
            row.add(String.valueOf(columnInfo.scale()));
            // radix
            row.add("10");
            // nullable
            row.add(columnInfo.nullable() ? String.valueOf(columnNullable) : String.valueOf(columnNoNulls));
            // remarks
            row.add(null);

            // COLUMN_DEF
            row.add("DEFAULT".equals(descTable.getString("default_type")) ? descTable.getString("default_expression")
                    : null);

            // "SQL_DATA_TYPE", unused per JavaDoc
            row.add(null);
            // "SQL_DATETIME_SUB", unused per JavaDoc
            row.add(null);

            // char octet length
            row.add("0");
            // ordinal
            row.add(String.valueOf(colNum));
            colNum += 1;

            // IS_NULLABLE
            row.add(columnInfo.nullable() ? "YES" : "NO");
            // "SCOPE_CATALOG",
            row.add(null);
            // "SCOPE_SCHEMA",
            row.add(null);
            // "SCOPE_TABLE",
            row.add(null);
            // "SOURCE_DATA_TYPE",
            row.add(null);
            // "IS_AUTOINCREMENT"
            row.add("NO");
            // "IS_GENERATEDCOLUMN"
            row.add("NO");

            rows.add(row);
        }
        descTable.close();
        List<List<Object>> columnValues = rowStoredToColumnStored(rows);
        Block header = setupHeader(rows.size(), new Column[] {
                new Column("TABLE_CAT", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("TABLE_SCHEM", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("TABLE_NAME", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("COLUMN_NAME", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("DATA_TYPE", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("TYPE_NAME", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("COLUMN_SIZE", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("BUFFER_LENGTH", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("DECIMAL_DIGITS", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("NUM_PREC_RADIX", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("NULLABLE", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("REMARKS", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("COLUMN_DEF", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("SQL_DATA_TYPE", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("SQL_DATETIME_SUB", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("CHAR_OCTET_LENGTH", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("ORDINAL_POSITION", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("IS_NULLABLE", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("SCOPE_CATALOG", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("SCOPE_SCHEMA", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("SCOPE_TABLE", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("SOURCE_DATA_TYPE", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("IS_AUTOINCREMENT", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("IS_GENERATEDCOLUMN", new DataTypeString(), columnValues.remove(0).toArray())
        });
        return new ClickHouseResultSet(header, new OneBlockIterator(header), new ClickHouseStatement(connection));
    }

    private ResultSet getEmptyResultSet() {
        Block header = new Block(0, new Column[] { new Column("some", new DataTypeString()) });
        return new ClickHouseResultSet(header, new OneBlockIterator(header), new ClickHouseStatement(connection));
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
            String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        List<List<Object>> rows = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        row.add("String");
        row.add(Types.VARCHAR);
        row.add(null); // precision
        row.add('\'');
        row.add('\'');
        row.add(null);
        row.add(typeNoNulls);
        row.add(true);
        row.add(typeSearchable);
        row.add(true); // unsigned
        row.add(true); // fixed precision (money)
        row.add(false);
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(10);
        rows.add(row);
        int[] sizes = { 8, 16, 32, 64 };
        boolean[] signed = { true, false };
        for (int size : sizes) {
            for (boolean b : signed) {
                row = new ArrayList<>();
                String name = (b ? "" : "U") + "Int" + size;
                row.add(name);
                row.add((size <= 16 ? Types.INTEGER : Types.BIGINT));
                row.add(null); // precision - todo
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(typeNoNulls);
                row.add(true);
                row.add(typePredBasic);
                row.add(!b); // unsigned
                row.add(true); // fixed precision (money)
                row.add(false); // auto-incr
                row.add(null);
                row.add(null);
                row.add(null); // scale - should be fixed
                row.add(null);
                row.add(null);
                row.add(10);
                rows.add(row);
            }
        }
        int[] floatSizes = { 32, 64 };
        for (int floatSize : floatSizes) {
            row = new ArrayList<>();
            String name = "Float" + floatSize;
            row.add(name);
            row.add(Types.FLOAT);
            row.add(null); // precision - todo
            row.add(null);
            row.add(null);
            row.add(null);
            row.add(typeNoNulls);
            row.add(true);
            row.add(typePredBasic);
            row.add(false); // unsigned
            row.add(true); // fixed precision (money)
            row.add(false); // auto-incr
            row.add(null);
            row.add(null);
            row.add(null); // scale - should be fixed
            row.add(null);
            row.add(null);
            row.add(10);
            rows.add(row);
        }
        row = new ArrayList<>();
        row.add("Date");
        row.add(Types.DATE);
        row.add(null); // precision - todo
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(typeNoNulls);
        row.add(true);
        row.add(typePredBasic);
        row.add(false); // unsigned
        row.add(true); // fixed precision (money)
        row.add(false); // auto-incr
        row.add(null);
        row.add(null);
        row.add(null); // scale - should be fixed
        row.add(null);
        row.add(null);
        row.add(10);
        rows.add(row);

        row = new ArrayList<>();
        row.add("DateTime");
        row.add(Types.TIMESTAMP);
        row.add(null); // precision - todo
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(typeNoNulls);
        row.add(true);
        row.add(typePredBasic);
        row.add(false); // unsigned
        row.add(true); // fixed precision (money)
        row.add(false); // auto-incr
        row.add(null);
        row.add(null);
        row.add(null); // scale - should be fixed
        row.add(null);
        row.add(null);
        row.add(10);
        rows.add(row);
        List<List<Object>> columnValues = rowStoredToColumnStored(rows);
        Block header = setupHeader(rows.size(), new Column[] {
                new Column("TYPE_NAME", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("DATA_TYPE", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("PRECISION", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("LITERAL_PREFIX", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("LITERAL_SUFFIX", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("CREATE_PARAMS", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("NULLABLE", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("CASE_SENSITIVE", new DataTypeInt8("Int8"), columnValues.remove(0).toArray()),
                new Column("SEARCHABLE", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("UNSIGNED_ATTRIBUTE", new DataTypeInt8("Int8"), columnValues.remove(0).toArray()),
                new Column("FIXED_PREC_SCALE", new DataTypeInt8("Int8"), columnValues.remove(0).toArray()),
                new Column("AUTO_INCREMENT", new DataTypeInt8("Int8"), columnValues.remove(0).toArray()),
                new Column("LOCAL_TYPE_NAME", new DataTypeString(), columnValues.remove(0).toArray()),
                new Column("MINIMUM_SCALE", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("MAXIMUM_SCALE", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("SQL_DATA_TYPE", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("SQL_DATETIME_SUB", new DataTypeInt32("Int32"), columnValues.remove(0).toArray()),
                new Column("NUM_PREC_RADIX", new DataTypeInt32("Int32"), columnValues.remove(0).toArray())
        });
        return new ClickHouseResultSet(header, new OneBlockIterator(header), new ClickHouseStatement(connection));
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY == type;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return ClickHouseVersionNumberUtil.getMajorVersion(connection.getServerVersion());
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return ClickHouseVersionNumberUtil.getMinorVersion(connection.getServerVersion());
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        return null;
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    private static Block setupHeader(int size, Column[] columns) {
        Block header = new Block(size, columns);
        header.initWriteBuffer();
        return header;
    }

}
