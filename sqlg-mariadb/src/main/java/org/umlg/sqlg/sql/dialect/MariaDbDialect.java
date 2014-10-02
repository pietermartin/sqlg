package org.umlg.sqlg.sql.dialect;

import com.tinkerpop.gremlin.structure.Property;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.umlg.sqlg.structure.*;

import java.sql.Types;
import java.util.*;

/**
 * Date: 2014/07/16
 * Time: 1:42 PM
 */
public class MariaDbDialect extends BaseSqlDialect implements SqlDialect {

    public MariaDbDialect(Configuration configurator) {
        super(configurator);
    }

    /**
     * MariaDb does not support schemas.
     * The public schema in this case will be equivalent to the database name
     * @return
     */
    @Override
    public String getPublicSchema() {
        return this.configurator.getString("mariadb.db");
    }

    @Override
    public boolean supportsCascade() {
        return false;
    }

    @Override
    public boolean supportSchemas() {
        return false;
    }

    @Override
    public Map<SchemaTable, Pair<Long, Long>> flushVertexCache(SqlG sqlG, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>>> vertexCache) {
        throw new UnsupportedOperationException("Batch processing is not supported by MariaDb.");
    }

    @Override
    public void flushEdgeCache(SqlG sqlG, Map<SchemaTable, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> edgeCache) {
        throw new UnsupportedOperationException("Batch processing is not supported by MariaDb.");
    }

    @Override
    public void flushVertexLabelCache(SqlG sqlG, Map<SqlgVertex, Pair<String, String>> vertexOutInLabelMap) {
        throw new UnsupportedOperationException("Batch processing is not supported by MariaDb.");
    }

    @Override
    public String getBatchNull() {
        throw new UnsupportedOperationException("Batch processing is not supported by MariaDb.");
    }

    @Override
    public void flushVertexPropertyCache(SqlG sqlG, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexPropertyCache) {
        throw new UnsupportedOperationException("Batch processing is not supported by MariaDb.");
    }

    @Override
    public void flushEdgePropertyCache(SqlG sqlG, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Map<String, Object>>>> edgePropertyCache) {
        throw new UnsupportedOperationException("Batch processing is not supported by MariaDb.");
    }

    @Override
    public void flushRemovedVertices(SqlG sqlG, Map<SchemaTable, List<SqlgVertex>> removeVertexCache) {
        throw new UnsupportedOperationException("Batch processing is not supported by MariaDb.");
    }

    @Override
    public void flushRemovedEdges(SqlG sqlG, Map<SchemaTable, List<SqlgEdge>> removeEdgeCache) {
        throw new UnsupportedOperationException("Batch processing is not supported by MariaDb.");
    }

    @Override
    public Set<String> getDefaultSchemas() {
        return new HashSet<>(Arrays.asList("information_schema", "performance_schema", "mysql"));
    }

    @Override
    public String getJdbcDriver() {
        return "org.mariadb.jdbc.Driver";
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
        if (value instanceof Float) {
            return;
        }
        if (value instanceof Double) {
            return;
        }
        if (value instanceof byte[]) {
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
        return "BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY";
    }

    @Override
    public String propertyTypeToSqlDefinition(PropertyType propertyType) {

        switch (propertyType) {
            case BOOLEAN:
                return "BOOLEAN";
            case BYTE:
                return "TINYINT";
            case SHORT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case LONG:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case STRING:
                return "TEXT";
            case BYTE_ARRAY:
                return "LONGBLOB";
            default:
                throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(propertyType);
        }
    }

    @Override
    public int propertyTypeToJavaSqlType(PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN:
                return Types.BOOLEAN;
            case BYTE:
                return Types.TINYINT;
            case SHORT:
                return Types.SMALLINT;
            case INTEGER:
                return Types.INTEGER;
            case LONG:
                return Types.BIGINT;
            case FLOAT:
                return Types.FLOAT;
            case DOUBLE:
                return Types.DOUBLE;
            case STRING:
                return Types.CLOB;
            case BYTE_ARRAY:
                return Types.LONGVARBINARY;
            default:
                throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(propertyType);
        }
    }

    @Override
    public PropertyType sqlTypeToPropertyType(int sqlType, String typeName) {
        switch (sqlType) {
            case Types.BIT:
                return PropertyType.BOOLEAN;
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
            case Types.VARCHAR:
                return PropertyType.STRING;
            case Types.VARBINARY:
                return PropertyType.BYTE_ARRAY;
            case Types.LONGVARBINARY:
                return PropertyType.BYTE_ARRAY;
            default:
                throw new IllegalStateException("Unknown sqlType " + sqlType);
        }
    }

    @Override
    public String getForeignKeyTypeDefinition() {
        return "BIGINT";
    }

    @Override
    public boolean supportsTransactionalSchema() {
        return false;
    }

    @Override
    public boolean supportsShortArrayValues() {
        return false;
    }

    @Override
    public boolean supportsBooleanArrayValues() {
        return false;
    }

    @Override
    public boolean supportsByteArrayValues() {
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
    public boolean supportsLongArrayValues() {
        return false;
    }

    @Override
    public boolean supportsStringArrayValues() {
        return false;
    }

    @Override
    public String getArrayDriverType(PropertyType booleanArray) {
        return null;
    }

    @Override
    public String existIndexQuery(SchemaTable schemaTable, String prefix, String indexName) {
        StringBuilder sb = new StringBuilder("SHOW INDEX FROM ");
        sb.append(schemaTable.getSchema());
        sb.append(".");
        sb.append(prefix);
        sb.append(schemaTable.getTable());
        sb.append(" WHERE Key_name = '");
        sb.append(indexName);
        sb.append("';");
        return sb.toString();
    }

}
