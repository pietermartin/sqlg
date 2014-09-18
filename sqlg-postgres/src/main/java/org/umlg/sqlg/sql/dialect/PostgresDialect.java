package org.umlg.sqlg.sql.dialect;

import com.mchange.v2.c3p0.C3P0ProxyConnection;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.*;

/**
 * Date: 2014/07/16
 * Time: 1:42 PM
 */
public class PostgresDialect extends BaseSqlDialect implements SqlDialect {

    private static final String COPY_COMMAND_SEPERATOR = "~";
    private Logger logger = LoggerFactory.getLogger(SqlG.class.getName());

    public PostgresDialect(Configuration configurator) {
        super(configurator);
    }

    @Override
    public boolean supportsBatchMode() {
        return true;
    }

    @Override
    public Set<String> getDefaultSchemas() {
        return new HashSet<>(Arrays.asList("pg_catalog", "public"));
    }

    @Override
    public String getJdbcDriver() {
        return "org.postgresql.xa.PGXADataSource";
    }

    @Override
    public String getForeignKeyTypeDefinition() {
        return "BIGINT";
    }

    @Override
    public String getColumnEscapeKey() {
        return "\"";
    }

    @Override
    public String getPrimaryKeyType() {
        return "BIGINT NOT NULL PRIMARY KEY";
    }

    @Override
    public String getAutoIncrementPrimaryKeyConstruct() {
        return "SERIAL PRIMARY KEY";
    }

    public void assertTableName(String tableName) {
        if (!StringUtils.isEmpty(tableName) && tableName.length() > 63) {
            throw new IllegalStateException(String.format("Postgres table names must be 63 characters or less! Given table name is %s", new String[]{tableName}));
        }
    }

    @Override
    public String getArrayDriverType(PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN_ARRAY:
                return "bool";
            case SHORT_ARRAY:
                return "smallint";
            case INTEGER_ARRAY:
                return "integer";
            case LONG_ARRAY:
                return "bigint";
            case FLOAT_ARRAY:
                return "float";
            case DOUBLE_ARRAY:
                return "float";
            case STRING_ARRAY:
                return "varchar";
            default:
                throw new IllegalStateException("propertyType " + propertyType.name() + " unknown!");
        }
    }

    @Override
    public String existIndexQuery(SchemaTable schemaTable, String prefix, String indexName) {
        StringBuilder sb = new StringBuilder("SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace");
        sb.append(" WHERE  c.relname = '");
        sb.append(indexName);
        sb.append("' AND n.nspname = '");
        sb.append(schemaTable.getSchema());
        sb.append("'");
        return sb.toString();
    }

    /**
     * flushes the cache via the copy command.
     * first writes the
     *
     * @param vertexCache A rather complex object.
     *                    The map's key is the vertex being cached.
     *                    The Triple holds,
     *                    1) The in labels
     *                    2) The out labels
     *                    3) The properties as a map of key values
     */
    @Override
    public Map<SchemaTable, Pair<Long, Long>> flushVertexCache(SqlG sqlG, Map<SchemaTable, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>> vertexCache) {
        Map<SchemaTable, Pair<Long, Long>> verticesRanges = new LinkedHashMap<>();
        C3P0ProxyConnection con = (C3P0ProxyConnection) sqlG.tx().getConnection();
        try {
            Method m = BaseConnection.class.getMethod("getCopyAPI", new Class[]{});
            Object[] arg = new Object[]{};
            CopyManager copyManager = (CopyManager) con.rawConnectionOperation(m, C3P0ProxyConnection.RAW_CONNECTION, arg);

            //first insert the VERTICES
            for (SchemaTable schemaTable : vertexCache.keySet()) {

                Map<SqlgVertex, Triple<String, String, Map<String, Object>>> vertices = vertexCache.get(schemaTable);

                Long copyCount;
                try (InputStream is = mapToVERTICES_InputStream(schemaTable, vertices)) {
                    StringBuilder sql = new StringBuilder();
                    sql.append("COPY ");
                    sql.append(maybeWrapInQoutes(getPublicSchema()));
                    sql.append(".");
                    sql.append(maybeWrapInQoutes(SchemaManager.VERTICES));
                    sql.append(" (");
                    sql.append(maybeWrapInQoutes(SchemaManager.VERTEX_SCHEMA));
                    sql.append(", ");
                    sql.append(maybeWrapInQoutes(SchemaManager.VERTEX_TABLE));
                    sql.append(", ");
                    sql.append(maybeWrapInQoutes(SchemaManager.VERTEX_OUT_LABELS));
                    sql.append(", ");
                    sql.append(maybeWrapInQoutes(SchemaManager.VERTEX_IN_LABELS));
                    sql.append(") FROM stdin WITH (DELIMITER '");
                    sql.append(COPY_COMMAND_SEPERATOR);
                    sql.append("')");
                    sql.append(";");
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug(sql.toString());
                    }
                    copyCount = copyManager.copyIn(sql.toString(), is);
                }
                Long endHigh;
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("copied in %d vertices", new Long[]{copyCount}));
                    logger.debug("SELECT CURRVAL('\"" + SchemaManager.VERTICES + "_ID_seq\"');");
                }
                try (PreparedStatement preparedStatement = con.prepareStatement("SELECT CURRVAL('\"" + SchemaManager.VERTICES + "_ID_seq\"');")) {
                    ResultSet resultSet = preparedStatement.executeQuery();
                    resultSet.next();
                    endHigh = resultSet.getLong(1);
                    resultSet.close();
                }

                //insert the labeled vertices
                try (InputStream is = mapToLabeledVertex_InputStream(endHigh, vertices)) {
                    StringBuffer sql = new StringBuffer();
                    sql.append("COPY ");
                    sql.append(maybeWrapInQoutes(schemaTable.getSchema()));
                    sql.append(".");
                    sql.append(maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + schemaTable.getTable()));
                    sql.append(" (\"ID\"");
                    for (Triple<String, String, Map<String, Object>> pair : vertices.values()) {
                        int count = 1;
                        for (String key : pair.getRight().keySet()) {
                            if (count++ <= pair.getRight().size()) {
                                sql.append(", ");
                            }
                            sql.append(maybeWrapInQoutes(key));
                        }
                        break;
                    }
                    sql.append(") ");
                    sql.append(" FROM stdin DELIMITER '");
                    sql.append(COPY_COMMAND_SEPERATOR);
                    sql.append("';");
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql.toString());
                    }
                    copyManager.copyIn(sql.toString(), is);
                }

                verticesRanges.put(schemaTable, Pair.of(endHigh - copyCount + 1, endHigh));

            }

            return verticesRanges;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flushEdgeCache(SqlG sqlG, Map<SchemaTable, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> edgeCache) {
        C3P0ProxyConnection con = (C3P0ProxyConnection) sqlG.tx().getConnection();
        try {
            Method m = BaseConnection.class.getMethod("getCopyAPI", new Class[]{});
            Object[] arg = new Object[]{};
            CopyManager copyManager = (CopyManager) con.rawConnectionOperation(m, C3P0ProxyConnection.RAW_CONNECTION, arg);

            //first insert the EDGES
            for (SchemaTable schemaTable : edgeCache.keySet()) {
                Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> triples = edgeCache.get(schemaTable);
                try (InputStream is = mapToEDGES_InputStream(schemaTable, triples)) {
                    StringBuilder sql = new StringBuilder();
                    sql.append("COPY ");
                    sql.append(maybeWrapInQoutes(this.getPublicSchema()));
                    sql.append(".");
                    sql.append(maybeWrapInQoutes(SchemaManager.EDGES));
                    sql.append(" (");
                    sql.append(maybeWrapInQoutes(SchemaManager.EDGE_SCHEMA));
                    sql.append(", ");
                    sql.append(maybeWrapInQoutes(SchemaManager.EDGE_TABLE));
                    sql.append(") FROM stdin DELIMITER '");
                    sql.append(COPY_COMMAND_SEPERATOR);
                    sql.append("';");
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug(sql.toString());
                    }
                    copyManager.copyIn(sql.toString(), is);
                }
                Long endHigh;
                try (PreparedStatement preparedStatement = con.prepareStatement("SELECT CURRVAL('\"" + SchemaManager.EDGES + "_ID_seq\"');")) {
                    ResultSet resultSet = preparedStatement.executeQuery();
                    resultSet.next();
                    endHigh = resultSet.getLong(1);
                    resultSet.close();
                }

                //insert the edges
                try (InputStream is = mapToEdge_InputStream(endHigh, triples)) {
                    StringBuffer sql = new StringBuffer();
                    sql.append("COPY ");
                    sql.append(maybeWrapInQoutes(schemaTable.getSchema()));
                    sql.append(".");
                    sql.append(maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + schemaTable.getTable()));
                    sql.append(" (\"ID\", ");
                    for (Triple<SqlgVertex, SqlgVertex, Map<String, Object>> triple : triples.values()) {
                        int count = 1;
                        sql.append(maybeWrapInQoutes(triple.getLeft().getSchema() + "." + triple.getLeft().getTable() + SqlgElement.OUT_VERTEX_COLUMN_END));
                        sql.append(", ");
                        sql.append(maybeWrapInQoutes(triple.getMiddle().getSchema() + "." + triple.getMiddle().getTable() + SqlgElement.IN_VERTEX_COLUMN_END));
                        for (String key : triple.getRight().keySet()) {
                            if (count++ <= triple.getRight().size()) {
                                sql.append(", ");
                            }
                            sql.append(maybeWrapInQoutes(key));
                        }
                        break;
                    }
                    sql.append(") ");
                    sql.append(" FROM stdin DELIMITER '");
                    sql.append(COPY_COMMAND_SEPERATOR);
                    sql.append("';");
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql.toString());
                    }
                    copyManager.copyIn(sql.toString(), is);
                }
            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flushVertexLabelCache(SqlG sqlG, Map<SqlgVertex, Pair<String, String>> vertexOutInLabelMap) {
        if (!vertexOutInLabelMap.isEmpty()) {
            Connection conn = sqlG.tx().getConnection();
            StringBuilder sql = new StringBuilder();
            sql.append("update \"VERTICES\" a\n" +
                    "SET (\"VERTEX_SCHEMA\", \"VERTEX_TABLE\", \"IN_LABELS\", \"OUT_LABELS\") =\n" +
                    "\t(v.\"VERTEX_SCHEMA\", v.\"VERTEX_TABLE\", v.\"IN_LABELS\", v.\"OUT_LABELS\")\n" +
                    "FROM ( \n" +
                    "    VALUES \n");
            int count = 1;
            for (SqlgVertex sqlgVertex : vertexOutInLabelMap.keySet()) {
                Pair<String, String> outInLabel = vertexOutInLabelMap.get(sqlgVertex);
                sql.append("        (");
                sql.append(sqlgVertex.id());
                sql.append(", '");
                sql.append(sqlgVertex.getSchema());
                sql.append("', '");
                sql.append(sqlgVertex.getTable());
                sql.append("', ");
                if (outInLabel.getRight() == null) {
                    sql.append("null");
                } else {
                    sql.append("'");
                    sql.append(outInLabel.getRight());
                    sql.append("'");
                }
                sql.append(", ");

                if (outInLabel.getLeft() == null) {
                    sql.append("null");
                } else {
                    sql.append("'");
                    sql.append(outInLabel.getLeft());
                    sql.append("'");
                }
                sql.append(")");
                if (count++ < vertexOutInLabelMap.size()) {
                    sql.append(",        \n");
                }
            }
            sql.append("\n) AS v(id, \"VERTEX_SCHEMA\", \"VERTEX_TABLE\", \"IN_LABELS\", \"OUT_LABELS\")");
            sql.append("\nWHERE a.\"ID\" = v.id");
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (Statement statement = conn.createStatement()) {
                statement.execute(sql.toString());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private InputStream mapToEdge_InputStream(Long endHigh, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> edgeCache) {
        Long start = endHigh - edgeCache.size() + 1;
        StringBuilder sb = new StringBuilder();
        int count = 1;
        for (Triple<SqlgVertex, SqlgVertex, Map<String, Object>> triple : edgeCache.values()) {
            sb.append(start++);
            sb.append(COPY_COMMAND_SEPERATOR);
            sb.append(triple.getLeft().id().toString());
            sb.append(COPY_COMMAND_SEPERATOR);
            sb.append(triple.getMiddle().id().toString());
            if (!triple.getRight().isEmpty()) {
                sb.append(COPY_COMMAND_SEPERATOR);
            }
            int countKeys = 1;
            for (String key : triple.getRight().keySet()) {
                Object value = triple.getRight().get(key);
                sb.append(value.toString());
                if (countKeys++ < triple.getRight().size()) {
                    sb.append(COPY_COMMAND_SEPERATOR);
                }
            }
            if (count++ < edgeCache.size()) {
                sb.append("\n");
            }
        }
        return new ByteArrayInputStream(sb.toString().getBytes());
    }

    private InputStream mapToLabeledVertex_InputStream(Long endHigh, Map<SqlgVertex, Triple<String, String, Map<String, Object>>> vertexCache) {
        //String str = "2,peter\n3,john";
        Long start = endHigh - vertexCache.size() + 1;
        StringBuilder sb = new StringBuilder();
        int count = 1;
        for (SqlgVertex sqlgVertex : vertexCache.keySet()) {
            Triple<String, String, Map<String, Object>> triple = vertexCache.get(sqlgVertex);
            //set the internal batch id to be used which inserting batch edges
            sqlgVertex.setInternalPrimaryKey(start);
            sb.append(start++);
            int countKeys = 1;
            for (String key : triple.getRight().keySet()) {
                if (countKeys++ <= triple.getRight().size()) {
                    sb.append(COPY_COMMAND_SEPERATOR);
                }
                Object value = triple.getRight().get(key);
                sb.append(value.toString());
            }
            if (count++ < vertexCache.size()) {
                sb.append("\n");
            }
        }
        return new ByteArrayInputStream(sb.toString().getBytes());
    }

    private InputStream mapToVERTICES_InputStream(SchemaTable schemaTable, Map<SqlgVertex, Triple<String, String, Map<String, Object>>> vertexCache) {
        StringBuilder sb = new StringBuilder();
        int count = 1;
        for (Triple<String, String, Map<String, Object>> triple : vertexCache.values()) {
            sb.append(schemaTable.getSchema());
            sb.append(COPY_COMMAND_SEPERATOR);
            sb.append(schemaTable.getTable());
            sb.append(COPY_COMMAND_SEPERATOR);
            //out labels
            sb.append(triple.getLeft());
            sb.append(COPY_COMMAND_SEPERATOR);
            //in labels
            sb.append(triple.getMiddle());
            if (count++ < vertexCache.size()) {
                sb.append("\n");
            }
        }
        return new ByteArrayInputStream(sb.toString().getBytes());
    }

    private InputStream mapToEDGES_InputStream(SchemaTable schemaTable, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> edgeCache) {
        StringBuilder sb = new StringBuilder();
        int count = 1;
        for (Triple<SqlgVertex, SqlgVertex, Map<String, Object>> triple : edgeCache.values()) {
            sb.append(schemaTable.getSchema());
            sb.append(COPY_COMMAND_SEPERATOR);
            sb.append(schemaTable.getTable());
            if (count++ < edgeCache.size()) {
                sb.append("\n");
            }
        }
        return new ByteArrayInputStream(sb.toString().getBytes());
    }

    @Override
    public String propertyTypeToSqlDefinition(PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN:
                return "BOOLEAN";
            case SHORT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case LONG:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case STRING:
                return "TEXT";
            case BYTE_ARRAY:
                return "BYTEA";
            case BOOLEAN_ARRAY:
                return "BOOLEAN[]";
            case SHORT_ARRAY:
                return "SMALLINT[]";
            case INTEGER_ARRAY:
                return "INTEGER[]";
            case LONG_ARRAY:
                return "BIGINT[]";
            case FLOAT_ARRAY:
                return "REAL[]";
            case DOUBLE_ARRAY:
                return "DOUBLE PRECISION[]";
            case STRING_ARRAY:
                return "TEXT[]";
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }

    @Override
    public PropertyType sqlTypeToPropertyType(int sqlType, String typeName) {
        switch (sqlType) {
            case Types.BIT:
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
            case Types.VARCHAR:
                return PropertyType.STRING;
            case Types.BINARY:
                return PropertyType.BYTE_ARRAY;
            case Types.ARRAY:
                switch (typeName) {
                    case "_bool":
                        return PropertyType.BOOLEAN_ARRAY;
                    case "_int2":
                        return PropertyType.SHORT_ARRAY;
                    case "_int4":
                        return PropertyType.INTEGER_ARRAY;
                    case "_int8":
                        return PropertyType.LONG_ARRAY;
                    case "_float4":
                        return PropertyType.FLOAT_ARRAY;
                    case "_float8":
                        return PropertyType.DOUBLE_ARRAY;
                    case "_text":
                        return PropertyType.STRING_ARRAY;
                    default:
                        throw new RuntimeException("Array type not supported " + typeName);
                }
            default:
                throw new IllegalStateException("Unknown sqlType " + sqlType);
        }
    }

    @Override
    public int propertyTypeToJavaSqlType(PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN:
                return Types.BOOLEAN;
            case SHORT:
                return Types.SMALLINT;
            case INTEGER:
                return Types.INTEGER;
            case LONG:
                return Types.BIGINT;
            case FLOAT:
                return Types.REAL;
            case DOUBLE:
                return Types.DOUBLE;
            case STRING:
                return Types.CLOB;
            case BYTE_ARRAY:
                return Types.ARRAY;
            case BOOLEAN_ARRAY:
                return Types.ARRAY;
            case SHORT_ARRAY:
                return Types.ARRAY;
            case INTEGER_ARRAY:
                return Types.ARRAY;
            case LONG_ARRAY:
                return Types.ARRAY;
            case FLOAT_ARRAY:
                return Types.ARRAY;
            case DOUBLE_ARRAY:
                return Types.ARRAY;
            case STRING_ARRAY:
                return Types.ARRAY;
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
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
        if (value instanceof float[]) {
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
        if (value instanceof Float[]) {
            return;
        }
        if (value instanceof Double[]) {
            return;
        }
        throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
    }

}
