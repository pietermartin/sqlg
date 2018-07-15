package org.umlg.sqlg.sql.dialect;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.time.*;
import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/07/07
 */
@SuppressWarnings("unused")
public class MariadbDialect extends BaseSqlDialect {

    @Override
    public int getMaximumSchemaNameLength() {
        return 63;
    }

    @Override
    public int getMaximumTableNameLength() {
        return 63;
    }

    @Override
    public int getMaximumColumnNameLength() {
        return 63;
    }

    @Override
    public int getMaximumIndexNameLength() {
        return 63;
    }

    @Override
    public boolean isMariaDb() {
        return true;
    }

    @Override
    public boolean supportsValuesExpression() {
        return false;
    }

    @Override
    public boolean supportsBatchMode() {
        return true;
    }

    @Override
    public boolean supportsSchemas() {
        return false;
    }

    @Override
    public String createSchemaStatement(String schemaName) {
        return "CREATE DATABASE IF NOT EXISTS " + maybeWrapInQoutes(schemaName) + " DEFAULT CHARACTER SET latin1 COLLATE latin1_general_cs";
    }

    @Override
    public boolean needsTemporaryTableSchema() {
        return true;
    }

    @Override
    public boolean supportsTemporaryTableOnCommitDrop() {
        return false;
    }

    @Override
    public String sqlInsertEmptyValues() {
        return " VALUES ()";
    }

    @Override
    public List<Triple<String, String, String>> getVertexTables(DatabaseMetaData metaData) {
        List<Triple<String, String, String>> vertexTables = new ArrayList<>();
        String[] types = new String[]{"TABLE"};
        try {
            //load the vertices
            try (ResultSet vertexRs = metaData.getTables(null, null, "V_%", types)) {
                while (vertexRs.next()) {
                    //MariaDb does not support schemas.
                    String tblCat = null;
                    String schema = vertexRs.getString(1);
                    String table = vertexRs.getString(3);

                    //verify the table name matches our pattern
                    if (!table.startsWith("V_")) {
                        continue;
                    }
                    vertexTables.add(Triple.of(tblCat, schema, table));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return vertexTables;
    }

    @Override
    public List<Triple<String, String, String>> getEdgeTables(DatabaseMetaData metaData) {
        List<Triple<String, String, String>> edgeTables = new ArrayList<>();
        String[] types = new String[]{"TABLE"};
        try {
            //load the edges without their properties
            try (ResultSet edgeRs = metaData.getTables(null, null, "E_%", types)) {
                while (edgeRs.next()) {
                    String edgCat = null;
                    String schema = edgeRs.getString(1);
                    String table = edgeRs.getString(3);
                    if (table.startsWith(Topology.EDGE_PREFIX)) {
                        edgeTables.add(Triple.of(edgCat, schema, table));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return edgeTables;
    }

    @Override
    public List<Triple<String, Integer, String>> getTableColumns(DatabaseMetaData metaData, String catalog, String schemaPattern,
                                                                 String tableNamePattern, String columnNamePattern) {
        List<Triple<String, Integer, String>> columns = new ArrayList<>();
        try (ResultSet rs = metaData.getColumns(schemaPattern, schemaPattern, tableNamePattern, columnNamePattern)) {
            while (rs.next()) {
                String columnName = rs.getString(4);
                int columnType = rs.getInt(5);
                String typeName = rs.getString("TYPE_NAME");
                columns.add(Triple.of(columnName, columnType, typeName));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return columns;
    }

    @Override
    public List<Triple<String, Boolean, String>> getIndexInfo(DatabaseMetaData metaData, String catalog,
                                                              String schema, String table, boolean unique, boolean approximate) {

        List<Triple<String, Boolean, String>> indexes = new ArrayList<>();
        try (ResultSet indexRs = metaData.getIndexInfo(schema, schema, table, false, true)) {
            while (indexRs.next()) {
                String indexName = indexRs.getString("INDEX_NAME");
                boolean nonUnique = indexRs.getBoolean("NON_UNIQUE");
                String columnName = indexRs.getString("COLUMN_NAME");
                indexes.add(Triple.of(indexName, nonUnique, columnName));
            }
            return indexes;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSchemaNames(DatabaseMetaData metaData) {
        List<String> schemaNames = new ArrayList<>();
        try {
            try (ResultSet schemaRs = metaData.getCatalogs()) {
                while (schemaRs.next()) {
                    String schema = schemaRs.getString(1);
                    if (!this.getInternalSchemas().contains(schema)) {
                        schemaNames.add(schema);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return schemaNames;
    }

    @Override
    public boolean schemaExists(DatabaseMetaData metadata, String schema) throws SQLException {
        ResultSet schemaRs = metadata.getCatalogs();
        while (schemaRs.next()) {
            String db = schemaRs.getString(1);
            if (db.equals(schema)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String maybeWrapInQoutes(String field) {
        return getColumnEscapeKey() + field.replace(getColumnEscapeKey(), "\"" + getColumnEscapeKey()) + getColumnEscapeKey();
//        return field.replace(getColumnEscapeKey(), "\"" + getColumnEscapeKey());
    }

    @Override
    public String dialectName() {
        return "MariadbDialect";
    }

    @Override
    public Set<String> getInternalSchemas() {
        return new HashSet<>(Arrays.asList("information_schema", "performance_schema", "mysql", "test"));
    }

    @Override
    public boolean supportsCascade() {
        return false;
    }

    @Override
    public String getPublicSchema() {
        return "PUBLIC";
    }

    @Override
    public String existIndexQuery(SchemaTable schemaTable, String prefix, String indexName) {
        StringBuilder sb = new StringBuilder("SELECT * FROM INFORMATION_SCHEMA.SYSTEM_INDEXINFO WHERE TABLE_SCHEM = '");
        sb.append(schemaTable.getSchema());
        sb.append("' AND  TABLE_NAME = '");
        sb.append(prefix);
        sb.append(schemaTable.getTable());
        sb.append("' AND INDEX_NAME = '");
        sb.append(indexName);
        sb.append("'");
        return sb.toString();
    }

    @Override
    public boolean supportsTransactionalSchema() {
        return false;
    }

    @Override
    public void validateProperty(Object key, Object value) {
        if (value instanceof String) {
            return;
        }
        if (value instanceof Character) {
            return;
        }
        if (value instanceof Boolean) {
            return;
        }
        if (value instanceof Byte) {
            return;
        }
        if (value instanceof Short) {
            return;
        }
        if (value instanceof Integer) {
            return;
        }
        if (value instanceof Long) {
            return;
        }
        if (value instanceof Double) {
            return;
        }
        if (value instanceof LocalDate) {
            return;
        }
        if (value instanceof LocalDateTime) {
            return;
        }
        if (value instanceof ZonedDateTime) {
            return;
        }
        if (value instanceof LocalTime) {
            return;
        }
        if (value instanceof Period) {
            return;
        }
        if (value instanceof Duration) {
            return;
        }
        if (value instanceof JsonNode) {
            return;
        }
        if (value instanceof byte[]) {
            return;
        }
        if (value instanceof boolean[]) {
            return;
        }
        if (value instanceof char[]) {
            return;
        }
        if (value instanceof short[]) {
            return;
        }
        if (value instanceof int[]) {
            return;
        }
        if (value instanceof long[]) {
            return;
        }
        if (value instanceof double[]) {
            return;
        }
        if (value instanceof String[]) {
            return;
        }
        if (value instanceof Character[]) {
            return;
        }
        if (value instanceof Boolean[]) {
            return;
        }
        if (value instanceof Byte[]) {
            return;
        }
        if (value instanceof Short[]) {
            return;
        }
        if (value instanceof Integer[]) {
            return;
        }
        if (value instanceof Long[]) {
            return;
        }
        if (value instanceof Double[]) {
            return;
        }
        if (value instanceof LocalDateTime[]) {
            return;
        }
        if (value instanceof LocalDate[]) {
            return;
        }
        if (value instanceof LocalTime[]) {
            return;
        }
        if (value instanceof ZonedDateTime[]) {
            return;
        }
        if (value instanceof Duration[]) {
            return;
        }
        if (value instanceof Period[]) {
            return;
        }
        if (value instanceof JsonNode[]) {
            return;
        }
        throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
    }

    @Override
    public String getColumnEscapeKey() {
        return "`";
    }

    @Override
    public String getPrimaryKeyType() {
        return "BIGINT NOT NULL PRIMARY KEY";
    }

    @Override
    public String getAutoIncrementPrimaryKeyConstruct() {
        return "SERIAL PRIMARY KEY";
    }

    @Override
    public String[] propertyTypeToSqlDefinition(PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN:
                return new String[]{"BOOLEAN"};
            case BYTE:
                return new String[]{"TINYINT"};
            case SHORT:
                return new String[]{"SMALLINT"};
            case INTEGER:
                return new String[]{"INTEGER"};
            case LONG:
                return new String[]{"BIGINT"};
            case DOUBLE:
                return new String[]{"DOUBLE"};
            case LOCALDATE:
                return new String[]{"DATE"};
            case LOCALDATETIME:
                //3 microseconds maps nicely to java's LocalDateTIme
                return new String[]{"DATETIME(3)"};
            case ZONEDDATETIME:
                return new String[]{"DATETIME(3)", "TINYTEXT"};
            case LOCALTIME:
                return new String[]{"TIME"};
            case PERIOD:
                return new String[]{"INTEGER", "INTEGER", "INTEGER"};
            case DURATION:
                return new String[]{"BIGINT", "INTEGER"};
            case STRING:
                return new String[]{"LONGTEXT"};
            case JSON:
                return new String[]{"LONGTEXT"};
            case POINT:
                throw new IllegalStateException("MariaDb does not support gis types!");
            case POLYGON:
                throw new IllegalStateException("MariaDb does not support gis types!");
            case GEOGRAPHY_POINT:
                throw new IllegalStateException("MariaDb does not support gis types!");
            case GEOGRAPHY_POLYGON:
                throw new IllegalStateException("MariaDb does not support gis types!");
            case BYTE_ARRAY:
                return new String[]{"BLOB"};
            case byte_ARRAY:
                return new String[]{"BLOB"};
            case boolean_ARRAY:
                return new String[]{"BOOLEAN ARRAY DEFAULT ARRAY[]"};
            default:
                throw SqlgExceptions.invalidPropertyType(propertyType);
        }
    }

    @Override
    public int[] propertyTypeToJavaSqlType(PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN:
                return new int[]{Types.BOOLEAN};
            case BYTE:
                return new int[]{Types.TINYINT};
            case SHORT:
                return new int[]{Types.SMALLINT};
            case INTEGER:
                return new int[]{Types.INTEGER};
            case LONG:
                return new int[]{Types.BIGINT};
            case DOUBLE:
                return new int[]{Types.DOUBLE};
            case STRING:
                return new int[]{Types.CLOB};
            case LOCALDATETIME:
                return new int[]{Types.TIMESTAMP};
            case LOCALDATE:
                return new int[]{Types.DATE};
            case LOCALTIME:
                return new int[]{Types.TIME};
            case ZONEDDATETIME:
                return new int[]{Types.TIMESTAMP, Types.CLOB};
            case PERIOD:
                return new int[]{Types.INTEGER, Types.INTEGER, Types.INTEGER};
            case DURATION:
                return new int[]{Types.BIGINT, Types.INTEGER};
            case JSON:
                return new int[]{Types.OTHER};
            case byte_ARRAY:
                return new int[]{Types.ARRAY};
            case BYTE_ARRAY:
                return new int[]{Types.ARRAY};
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }

    @Override
    public PropertyType sqlTypeToPropertyType(SqlgGraph sqlgGraph, String schema, String table, String column, int sqlType, String typeName, ListIterator<Triple<String, Integer, String>> metaDataIter) {
        switch (sqlType) {
            case Types.BOOLEAN:
                return PropertyType.BOOLEAN;
            case Types.SMALLINT:
                return PropertyType.SHORT;
            case Types.INTEGER:
                return PropertyType.INTEGER;
            case Types.BIGINT:
                return PropertyType.LONG;
            case Types.REAL:
                return PropertyType.FLOAT;
            case Types.DOUBLE:
                return PropertyType.DOUBLE;
            case Types.LONGVARCHAR:
                return PropertyType.STRING;
            case Types.TIMESTAMP:
                return PropertyType.LOCALDATETIME;
            case Types.DATE:
                return PropertyType.LOCALDATE;
            case Types.TIME:
                return PropertyType.LOCALTIME;
            case Types.VARBINARY:
                return PropertyType.BYTE_ARRAY;
            case Types.ARRAY:
                return sqlArrayTypeNameToPropertyType(typeName, sqlgGraph, schema, table, column, metaDataIter);
            default:
                throw new IllegalStateException("Unknown sqlType " + sqlType);
        }
    }

    @Override
    public PropertyType sqlArrayTypeNameToPropertyType(String typeName, SqlgGraph sqlgGraph, String schema, String table, String columnName, ListIterator<Triple<String, Integer, String>> metaDataIter) {
        switch (typeName) {
            case "BOOLEAN ARRAY":
                return PropertyType.BOOLEAN_ARRAY;
            case "SMALLINT ARRAY":
                return PropertyType.SHORT_ARRAY;
            case "INTEGER ARRAY":
                return PropertyType.INTEGER_ARRAY;
            case "BIGINT ARRAY":
                return PropertyType.LONG_ARRAY;
            case "DOUBLE ARRAY":
                return PropertyType.DOUBLE_ARRAY;
            case "DATE ARRAY":
                return PropertyType.LOCALDATE_ARRAY;
            case "TIME WITH TIME ZONE ARRAY":
                return PropertyType.LOCALTIME_ARRAY;
            case "TIMESTAMP WITH TIME ZONE ARRAY":
                //need to check the next column to know if its a LocalDateTime or ZonedDateTime array
                Triple<String, Integer, String> metaData = metaDataIter.next();
                metaDataIter.previous();
                if (metaData.getLeft().startsWith(columnName + "~~~")) {
                    return PropertyType.ZONEDDATETIME_ARRAY;
                } else {
                    return PropertyType.LOCALDATETIME_ARRAY;
                }
            default:
                if (typeName.contains("VARCHAR") && typeName.contains("ARRAY")) {
                    return PropertyType.STRING_ARRAY;
                } else {
                    throw new RuntimeException(String.format("Array type not supported typeName = %s", typeName));
                }
        }
    }

    @Override
    public String getForeignKeyTypeDefinition() {
        return "BIGINT UNSIGNED";
    }

    @Override
    public String getArrayDriverType(PropertyType propertyType) {
        switch (propertyType) {
            case boolean_ARRAY:
                return "BOOLEAN";
            case BOOLEAN_ARRAY:
                return "BOOLEAN";
            case SHORT_ARRAY:
                return "SMALLINT";
            case short_ARRAY:
                return "SMALLINT";
            case INTEGER_ARRAY:
                return "INTEGER";
            case int_ARRAY:
                return "INTEGER";
            case LONG_ARRAY:
                return "BIGINT";
            case long_ARRAY:
                return "BIGINT";
            case DOUBLE_ARRAY:
                return "DOUBLE";
            case double_ARRAY:
                return "DOUBLE";
            case STRING_ARRAY:
                return "VARCHAR";
            case LOCALDATETIME_ARRAY:
                return "TIMESTAMP";
            case LOCALDATE_ARRAY:
                return "DATE";
            case LOCALTIME_ARRAY:
                return "TIME";
            default:
                throw new IllegalStateException("propertyType " + propertyType.name() + " unknown!");
        }
    }

    @Override
    public String createTableStatement() {
        return "CREATE TABLE ";
    }

    @Override
    public void validateColumnName(String column) {
        super.validateColumnName(column);
    }

    @Override
    public Set<String> getSpacialRefTable() {
        return Collections.emptySet();
    }

    @Override
    public List<String> getGisSchemas() {
        return Collections.emptyList();
    }

    @Override
    public void lockTable(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix) {
        throw new UnsupportedOperationException("MariaDb does not support table locking!");
    }

    @Override
    public void alterSequenceCacheSize(SqlgGraph sqlgGraph, SchemaTable schemaTable, String sequence, int batchSize) {
        throw new UnsupportedOperationException("MariaDb does not support alterSequenceCacheSize!");
    }

    @Override
    public long nextSequenceVal(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix) {
        throw new UnsupportedOperationException("MariaDb does not support nextSequenceVal!");
    }

    @Override
    public long currSequenceVal(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix) {
        throw new UnsupportedOperationException("MariaDb does not support currSequenceVal!");
    }

    @Override
    public String sequenceName(SqlgGraph sqlgGraph, SchemaTable outSchemaTable, String prefix) {
        throw new UnsupportedOperationException("MariaDb does not support sequenceName!");
    }

    @Override
    public boolean supportsBulkWithinOut() {
        return false;
    }

    @Override
    public String createTemporaryTableStatement() {
        return "CREATE TEMPORARY TABLE " + maybeWrapInQoutes(getPublicSchema()) + ".";
    }

    @Override
    public String afterCreateTemporaryTableStatement() {
        return "";
    }

    @Override
    public List<String> sqlgTopologyCreationScripts() {
        List<String> result = new ArrayList<>();

        //SERIAL is an alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE
        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`V_graph` (" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`createdOn` DATETIME, " +
                "`updatedOn` DATETIME, " +
                "`version` TEXT, " +
                "`dbVersion` TEXT);");
        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`V_schema` (" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`createdOn` DATETIME, " +
                "`name` TEXT);");
        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`V_vertex` (" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`createdOn` DATETIME, " +
                "`name` TEXT, " +
                "`schemaVertex` TEXT, " +
                "`partitionType` TEXT, " +
                "`partitionExpression` TEXT, " +
                "`shardCount` INTEGER);");
        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`V_edge` (" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`createdOn` DATETIME, " +
                "`name` TEXT, " +
                "`partitionType` TEXT, " +
                "`partitionExpression` TEXT, " +
                "`shardCount` INTEGER);");
        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`V_partition` (" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`createdOn` DATETIME, " +
                "`name` TEXT," +
                "`from` TEXT, " +
                "`to` TEXT, " +
                "`in` TEXT, " +
                "`partitionType` TEXT, " +
                "`partitionExpression` TEXT);");
        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`V_property` (" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`createdOn` DATETIME, " +
                "`name` TEXT, " +
                "`type` TEXT);");
        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`V_index` (" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`createdOn` DATETIME, " +
                "`name` TEXT, " +
                "`index_type` TEXT);");
        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`V_globalUniqueIndex` (" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`createdOn` DATETIME, " +
                "`name` TEXT);");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`E_schema_vertex`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.vertex__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.schema__O` BIGINT UNSIGNED, " +
                "FOREIGN KEY (`sqlg_schema.vertex__I`) REFERENCES `sqlg_schema`.`V_vertex` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.schema__O`) REFERENCES `sqlg_schema`.`V_schema` (`ID`)" +
                ");");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`E_in_edges`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.edge__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.vertex__O` BIGINT UNSIGNED, " +
                "FOREIGN KEY (`sqlg_schema.edge__I`) REFERENCES `sqlg_schema`.`V_edge` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.vertex__O`) REFERENCES `sqlg_schema`.`V_vertex` (`ID`)" +
                ");");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`E_out_edges`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.edge__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.vertex__O` BIGINT UNSIGNED, " +
                "FOREIGN KEY (`sqlg_schema.edge__I`) REFERENCES `sqlg_schema`.`V_edge` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.vertex__O`) REFERENCES `sqlg_schema`.`V_vertex` (`ID`)" +
                ");");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`E_vertex_property`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.property__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.vertex__O` BIGINT UNSIGNED, " +
                "FOREIGN KEY (`sqlg_schema.property__I`) REFERENCES `sqlg_schema`.`V_property` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.vertex__O`) REFERENCES `sqlg_schema`.`V_vertex` (`ID`)" +
                ");");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`E_edge_property`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.property__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.edge__O` BIGINT UNSIGNED, " +
                "FOREIGN KEY (`sqlg_schema.property__I`) REFERENCES `sqlg_schema`.`V_property` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.edge__O`) REFERENCES `sqlg_schema`.`V_edge` (`ID`)" +
                ");");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`" + Topology.EDGE_PREFIX + "vertex_identifier`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.property__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.vertex__O` BIGINT UNSIGNED, " +
                "`identifier_index` INTEGER, " +
                "FOREIGN KEY (`sqlg_schema.property__I`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "property` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.vertex__O`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "vertex` (`ID`));");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`" + Topology.EDGE_PREFIX + "edge_identifier`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.property__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.edge__O` BIGINT UNSIGNED, " +
                "`identifier_index` INTEGER, " +
                "FOREIGN KEY (`sqlg_schema.property__I`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "property` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.edge__O`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "edge` (`ID`));");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`E_vertex_partition`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.partition__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.vertex__O` BIGINT UNSIGNED, " +
                "FOREIGN KEY (`sqlg_schema.partition__I`) REFERENCES `sqlg_schema`.`V_partition` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.vertex__O`) REFERENCES `sqlg_schema`.`V_vertex` (`ID`));");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`E_edge_partition`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.partition__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.edge__O` BIGINT UNSIGNED, " +
                "FOREIGN KEY (`sqlg_schema.partition__I`) REFERENCES `sqlg_schema`.`V_partition` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.edge__O`) REFERENCES `sqlg_schema`.`V_edge` (`ID`));");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`E_partition_partition`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.partition__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.partition__O` BIGINT UNSIGNED, " +
                "FOREIGN KEY (`sqlg_schema.partition__I`) REFERENCES `sqlg_schema`.`V_partition` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.partition__O`) REFERENCES `sqlg_schema`.`V_partition` (`ID`));");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`" + Topology.EDGE_PREFIX + "vertex_distribution`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.property__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.vertex__O` BIGINT UNSIGNED, " +
                "FOREIGN KEY (`sqlg_schema.property__I`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "property` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.vertex__O`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "vertex` (`ID`));");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`" + Topology.EDGE_PREFIX + "vertex_colocate`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.vertex__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.vertex__O` BIGINT UNSIGNED, " +
                "FOREIGN KEY (`sqlg_schema.vertex__I`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "vertex` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.vertex__O`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "vertex` (`ID`));");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`" + Topology.EDGE_PREFIX + "edge_distribution`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.property__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.edge__O` BIGINT UNSIGNED, " +
                "FOREIGN KEY (`sqlg_schema.property__I`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "property` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.edge__O`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "edge` (`ID`));");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`" + Topology.EDGE_PREFIX + "edge_colocate`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.vertex__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.edge__O` BIGINT UNSIGNED, " +
                "FOREIGN KEY (`sqlg_schema.vertex__I`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "vertex` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.edge__O`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "edge` (`ID`));");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`E_vertex_index`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.index__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.vertex__O` BIGINT UNSIGNED, " +
                "FOREIGN KEY (`sqlg_schema.index__I`) REFERENCES `sqlg_schema`.`V_index` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.vertex__O`) REFERENCES `sqlg_schema`.`V_vertex` (`ID`)" +
                ");");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`E_edge_index`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.index__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.edge__O` BIGINT UNSIGNED, " +
                "FOREIGN KEY (`sqlg_schema.index__I`) REFERENCES `sqlg_schema`.`V_index` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.edge__O`) REFERENCES `sqlg_schema`.`V_edge` (`ID`)" +
                ");");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`E_index_property`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.property__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.index__O` BIGINT UNSIGNED, " +
                "`sequence` INTEGER, " +
                "FOREIGN KEY (`sqlg_schema.property__I`) REFERENCES `sqlg_schema`.`V_property` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.index__O`) REFERENCES `sqlg_schema`.`V_index` (`ID`)" +
                ");");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`E_globalUniqueIndex_property`(" +
                "`ID` SERIAL PRIMARY KEY, " +
                "`sqlg_schema.property__I` BIGINT UNSIGNED, " +
                "`sqlg_schema.globalUniqueIndex__O` BIGINT UNSIGNED, " +
                "FOREIGN KEY (`sqlg_schema.property__I`) REFERENCES `sqlg_schema`.`V_property` (`ID`), " +
                "FOREIGN KEY (`sqlg_schema.globalUniqueIndex__O`) REFERENCES `sqlg_schema`.`V_globalUniqueIndex` (`ID`)" +
                ");");

        result.add("CREATE TABLE IF NOT EXISTS `sqlg_schema`.`V_log`(`ID` SERIAL PRIMARY KEY, `timestamp` DATETIME, `pid` INTEGER, `log` TEXT);");
        return result;
    }

    @Override
    public String sqlgCreateTopologyGraph() {
        return "CREATE TABLE IF NOT EXISTS `sqlg_schema`.`V_graph` (`ID` SERIAL PRIMARY KEY, `createdOn` DATETIME, `updatedOn` DATETIME, `version` TEXT, `dbVersion` TEXT);";
    }

    @Override
    public String sqlgAddIndexEdgeSequenceColumn() {
        return "ALTER TABLE `sqlg_schema`.`E_index_property` ADD COLUMN `sequence` INTEGER DEFAULT 0;";
    }

    private Array createArrayOf(Connection conn, PropertyType propertyType, Object[] data) {
        try {
            switch (propertyType) {
                case LOCALTIME_ARRAY:
                case STRING_ARRAY:
                case long_ARRAY:
                case LONG_ARRAY:
                case int_ARRAY:
                case INTEGER_ARRAY:
                case short_ARRAY:
                case SHORT_ARRAY:
                case float_ARRAY:
                case FLOAT_ARRAY:
                case double_ARRAY:
                case DOUBLE_ARRAY:
                case boolean_ARRAY:
                case BOOLEAN_ARRAY:
                case LOCALDATETIME_ARRAY:
                case LOCALDATE_ARRAY:
                case ZONEDDATETIME_ARRAY:
                case JSON_ARRAY:
                    return conn.createArrayOf(getArrayDriverType(propertyType), data);
                default:
                    throw new IllegalStateException("Unhandled array type " + propertyType.name());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object convertArray(PropertyType propertyType, java.sql.Array array) throws SQLException {
        switch (propertyType) {
            case BOOLEAN_ARRAY:
                return SqlgUtil.convertObjectArrayToBooleanArray((Object[]) array.getArray());
            case boolean_ARRAY:
                return SqlgUtil.convertObjectArrayToBooleanPrimitiveArray((Object[]) array.getArray());
            case SHORT_ARRAY:
                return SqlgUtil.convertObjectOfIntegersArrayToShortArray((Object[]) array.getArray());
            case short_ARRAY:
                return SqlgUtil.convertObjectOfIntegersArrayToShortPrimitiveArray((Object[]) array.getArray());
            case INTEGER_ARRAY:
                return SqlgUtil.convertObjectOfIntegersArrayToIntegerArray((Object[]) array.getArray());
            case int_ARRAY:
                return SqlgUtil.convertObjectOfIntegersArrayToIntegerPrimitiveArray((Object[]) array.getArray());
            case LONG_ARRAY:
                return SqlgUtil.convertObjectOfLongsArrayToLongArray((Object[]) array.getArray());
            case long_ARRAY:
                return SqlgUtil.convertObjectOfLongsArrayToLongPrimitiveArray((Object[]) array.getArray());
            case DOUBLE_ARRAY:
                return SqlgUtil.convertObjectOfDoublesArrayToDoubleArray((Object[]) array.getArray());
            case double_ARRAY:
                return SqlgUtil.convertObjectOfDoublesArrayToDoublePrimitiveArray((Object[]) array.getArray());
            case FLOAT_ARRAY:
                return SqlgUtil.convertObjectOfFloatsArrayToFloatArray((Object[]) array.getArray());
            case float_ARRAY:
                return SqlgUtil.convertObjectOfFloatsArrayToFloatPrimitiveArray((Object[]) array.getArray());
            case STRING_ARRAY:
                return SqlgUtil.convertObjectOfStringsArrayToStringArray((Object[]) array.getArray());
            case LOCALDATETIME_ARRAY:
                Object[] timestamps = (Object[]) array.getArray();
                return SqlgUtil.copyObjectArrayOfTimestampToLocalDateTime(timestamps, new LocalDateTime[(timestamps).length]);
            case LOCALDATE_ARRAY:
                Object[] dates = (Object[]) array.getArray();
                return SqlgUtil.copyObjectArrayOfDateToLocalDate(dates, new LocalDate[dates.length]);
            case LOCALTIME_ARRAY:
                Object[] times = (Object[]) array.getArray();
                return SqlgUtil.copyObjectArrayOfTimeToLocalTime(times, new LocalTime[times.length]);
            default:
                throw new IllegalStateException("Unhandled property type " + propertyType.name());
        }
    }

    @Override
    public void setArray(PreparedStatement statement, int index, PropertyType type,
                         Object[] values) throws SQLException {
        statement.setArray(index, createArrayOf(statement.getConnection(), type, values));
    }

    @Override
    public boolean isSystemIndex(String indexName) {
        return indexName.contains("_ibfk_") || indexName.equals("PRIMARY") ||
                indexName.endsWith(Topology.IN_VERTEX_COLUMN_END) || indexName.endsWith(Topology.OUT_VERTEX_COLUMN_END);
    }

    @Override
    public boolean supportsBooleanArrayValues() {
        return false;
    }

    @Override
    public boolean supportsDoubleArrayValues() {
        return false;
    }

    @Override
    public boolean supportsFloatArrayValues() {
        return false;
    }

    @Override
    public boolean supportsIntegerArrayValues() {
        return false;
    }

    @Override
    public boolean supportsShortArrayValues() {
        return false;
    }

    @Override
    public boolean supportsLongArrayValues() {
        return false;
    }

    @Override
    public boolean supportsStringArrayValues() {
        return false;
    }

    @Override
    public boolean supportsFloatValues() {
        return false;
    }

    @Override
    public boolean supportsByteValues() {
        return false;
    }

    @Override
    public boolean supportsLocalDateTimeArrayValues() {
        return false;
    }

    @Override
    public boolean supportsLocalTimeArrayValues() {
        return false;
    }

    @Override
    public boolean supportsLocalDateArrayValues() {
        return false;
    }

    @Override
    public boolean supportsZonedDateTimeArrayValues() {
        return false;
    }

    @Override
    public boolean supportsPeriodArrayValues() {
        return false;
    }

    @Override
    public boolean supportsDurationArrayValues() {
        return false;
    }

    @Override
    public boolean requiresIndexLengthLimit() {
        return true;
    }

    @Override
    public String valueToValuesString(PropertyType propertyType, Object value) {
        throw new RuntimeException("Not yet implemented");
    }

    @Override
    public boolean supportsType(PropertyType propertyType) {
        return false;
    }

    @Override
    public String sqlToTurnOffReferentialConstraintCheck(String tableName) {
        return "SET FOREIGN_KEY_CHECKS=0";
    }

    @Override
    public String sqlToTurnOnReferentialConstraintCheck(String tableName) {
        return "SET FOREIGN_KEY_CHECKS=1";
    }

    @Override
    public String addDbVersionToGraph(DatabaseMetaData metadata) {
        return "ALTER TABLE `sqlg_schema`.`V_graph` ADD COLUMN `dbVersion` TEXT;";
    }

    @Override
    public List<String> addPartitionTables() {
        return Arrays.asList(
                "ALTER TABLE `sqlg_schema`.`V_vertex` ADD COLUMN `partitionType` TEXT;",
                "UPDATE `sqlg_schema`.`V_vertex` SET `partitionType` = 'NONE';",
                "ALTER TABLE `sqlg_schema`.`V_vertex` ADD COLUMN `partitionExpression` TEXT;",
                "ALTER TABLE `sqlg_schema`.`V_vertex` ADD COLUMN `shardCount` INTEGER;",
                "ALTER TABLE `sqlg_schema`.`V_edge` ADD COLUMN `partitionType` TEXT;",
                "UPDATE `sqlg_schema`.`V_edge` SET `partitionType` = 'NONE';",
                "ALTER TABLE `sqlg_schema`.`V_edge` ADD COLUMN `partitionExpression` TEXT;",
                "ALTER TABLE `sqlg_schema`.`V_edge` ADD COLUMN `shardCount` INTEGER;",
                "CREATE TABLE IF NOT EXISTS `sqlg_schema`.`V_partition` (" +
                        "`ID` SERIAL PRIMARY KEY, " +
                        "`createdOn` DATETIME, " +
                        "`name` TEXT, " +
                        "`from` TEXT, " +
                        "`to` TEXT, " +
                        "`in` TEXT, " +
                        "`partitionType` TEXT, " +
                        "`partitionExpression` TEXT);",
                "CREATE TABLE IF NOT EXISTS `sqlg_schema`.`E_vertex_partition`(" +
                        "`ID` SERIAL PRIMARY KEY, " +
                        "`sqlg_schema.partition__I` BIGINT UNSIGNED, " +
                        "`sqlg_schema.vertex__O` BIGINT UNSIGNED, " +
                        "FOREIGN KEY (`sqlg_schema.partition__I`) REFERENCES `sqlg_schema`.`V_partition` (`ID`), " +
                        "FOREIGN KEY (`sqlg_schema.vertex__O`) REFERENCES `sqlg_schema`.`V_vertex` (`ID`));",
                "CREATE TABLE IF NOT EXISTS `sqlg_schema`.`E_edge_partition`(" +
                        "`ID` SERIAL PRIMARY KEY, " +
                        "`sqlg_schema.partition__I` BIGINT UNSIGNED, " +
                        "`sqlg_schema.edge__O` BIGINT UNSIGNED, " +
                        "FOREIGN KEY (`sqlg_schema.partition__I`) REFERENCES `sqlg_schema`.`V_partition` (`ID`), " +
                        "FOREIGN KEY (`sqlg_schema.edge__O`) REFERENCES `sqlg_schema`.`V_edge` (`ID`));",

                "CREATE TABLE IF NOT EXISTS `sqlg_schema`.`E_partition_partition`(" +
                        "`ID` SERIAL PRIMARY KEY, " +
                        "`sqlg_schema.partition__I` BIGINT UNSIGNED, " +
                        "`sqlg_schema.partition__O` BIGINT UNSIGNED, " +
                        "FOREIGN KEY (`sqlg_schema.partition__I`) REFERENCES `sqlg_schema`.`V_partition` (`ID`), " +
                        "FOREIGN KEY (`sqlg_schema.partition__O`) REFERENCES `sqlg_schema`.`V_partition` (`ID`));",

                "CREATE TABLE IF NOT EXISTS `sqlg_schema`.`" + Topology.EDGE_PREFIX + "vertex_identifier`(" +
                        "`ID` SERIAL PRIMARY KEY, " +
                        "`sqlg_schema.property__I` BIGINT UNSIGNED, " +
                        "`sqlg_schema.vertex__O` BIGINT UNSIGNED, " +
                        "`identifier_index` INTEGER, " +
                        "FOREIGN KEY (`sqlg_schema.property__I`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "property` (`ID`), " +
                        "FOREIGN KEY (`sqlg_schema.vertex__O`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "vertex` (`ID`));",

                "CREATE TABLE IF NOT EXISTS `sqlg_schema`.`" + Topology.EDGE_PREFIX + "edge_identifier`(" +
                        "`ID` SERIAL PRIMARY KEY, " +
                        "`sqlg_schema.property__I` BIGINT UNSIGNED, " +
                        "`sqlg_schema.edge__O` BIGINT UNSIGNED, " +
                        "`identifier_index` INTEGER, " +
                        "FOREIGN KEY (`sqlg_schema.property__I`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "property` (`ID`), " +
                        "FOREIGN KEY (`sqlg_schema.edge__O`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "edge` (`ID`));",

                "CREATE TABLE IF NOT EXISTS `sqlg_schema`.`" + Topology.EDGE_PREFIX + "vertex_distribution`(" +
                        "`ID` SERIAL PRIMARY KEY, " +
                        "`sqlg_schema.property__I` BIGINT UNSIGNED, " +
                        "`sqlg_schema.vertex__O` BIGINT UNSIGNED, " +
                        "FOREIGN KEY (`sqlg_schema.property__I`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "property` (`ID`), " +
                        "FOREIGN KEY (`sqlg_schema.vertex__O`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "vertex` (`ID`));",

                "CREATE TABLE IF NOT EXISTS `sqlg_schema`.`" + Topology.EDGE_PREFIX + "vertex_colocate`(" +
                        "`ID` SERIAL PRIMARY KEY, " +
                        "`sqlg_schema.vertex__I` BIGINT UNSIGNED, " +
                        "`sqlg_schema.vertex__O` BIGINT UNSIGNED, " +
                        "FOREIGN KEY (`sqlg_schema.vertex__I`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "vertex` (`ID`), " +
                        "FOREIGN KEY (`sqlg_schema.vertex__O`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "vertex` (`ID`));",

                "CREATE TABLE IF NOT EXISTS `sqlg_schema`.`" + Topology.EDGE_PREFIX + "edge_distribution`(" +
                        "`ID` SERIAL PRIMARY KEY, " +
                        "`sqlg_schema.property__I` BIGINT UNSIGNED, " +
                        "`sqlg_schema.edge__O` BIGINT UNSIGNED, " +
                        "FOREIGN KEY (`sqlg_schema.property__I`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "property` (`ID`), " +
                        "FOREIGN KEY (`sqlg_schema.edge__O`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "edge` (`ID`));",

                "CREATE TABLE IF NOT EXISTS `sqlg_schema`.`" + Topology.EDGE_PREFIX + "edge_colocate`(" +
                        "`ID` SERIAL PRIMARY KEY, " +
                        "`sqlg_schema.vertex__I` BIGINT UNSIGNED, " +
                        "`sqlg_schema.edge__O` BIGINT UNSIGNED, " +
                        "FOREIGN KEY (`sqlg_schema.vertex__I`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "vertex` (`ID`), " +
                        "FOREIGN KEY (`sqlg_schema.edge__O`) REFERENCES `sqlg_schema`.`" + Topology.VERTEX_PREFIX + "edge` (`ID`));"
        );
    }

    /**
     * Hardcoded the rows to return. MariaDB does nto support just an offset.
     *
     * @param skip The number of rows to skip. i.e. OFFSET
     * @return The sql fragment.
     */
    @Override
    public String getSkipClause(long skip) {
        return " LIMIT " + skip + ", 1000000";
    }
}
