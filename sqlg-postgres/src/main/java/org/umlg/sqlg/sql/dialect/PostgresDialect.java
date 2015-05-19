package org.umlg.sqlg.sql.dialect;

import com.mchange.v2.c3p0.C3P0ProxyConnection;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
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

    private static final String BATCH_NULL = "\\N";
    private static final String COPY_COMMAND_SEPARATOR = "\t";
    private static final int PARAMETER_LIMIT = 32767;
    private Logger logger = LoggerFactory.getLogger(SqlgGraph.class.getName());

    public PostgresDialect(Configuration configurator) {
        super(configurator);
    }

    @Override
    public boolean supportsBatchMode() {
        return true;
    }

    @Override
    public Set<String> getDefaultSchemas() {
        return new HashSet<>(Arrays.asList("pg_catalog", "public", "information_schema"));
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
    public Map<SchemaTable, Pair<Long, Long>> flushVertexCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>>> vertexCache) {
        Map<SchemaTable, Pair<Long, Long>> verticesRanges = new LinkedHashMap<>();
        C3P0ProxyConnection con = (C3P0ProxyConnection) sqlgGraph.tx().getConnection();
        try {
            Method m = BaseConnection.class.getMethod("getCopyAPI", new Class[]{});
            Object[] arg = new Object[]{};
            CopyManager copyManager = (CopyManager) con.rawConnectionOperation(m, C3P0ProxyConnection.RAW_CONNECTION, arg);
            for (SchemaTable schemaTable : vertexCache.keySet()) {
                Pair<SortedSet<String>, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>> vertices = vertexCache.get(schemaTable);
                //insert the labeled vertices
                long endHigh;
                long numberInserted;
                try (InputStream is = mapToLabeledVertex_InputStream(vertices)) {
                    StringBuffer sql = new StringBuffer();
                    sql.append("COPY ");
                    sql.append(maybeWrapInQoutes(schemaTable.getSchema()));
                    sql.append(".");
                    sql.append(maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + schemaTable.getTable()));
                    sql.append(" (");
                    if (vertices.getLeft().isEmpty()) {
                        //copy command needs at least one field.
                        //check if the dummy field exist, if not create it
                        sqlgGraph.getSchemaManager().ensureColumnExist(
                                schemaTable.getSchema(),
                                SchemaManager.VERTEX_PREFIX + schemaTable.getTable(),
                                ImmutablePair.of("_copy_dummy", PropertyType.from(0)));
                        sql.append(maybeWrapInQoutes("_copy_dummy"));
                    } else {
                        int count = 1;
                        for (String key : vertices.getLeft()) {
                            if (count > 1 && count <= vertices.getLeft().size()) {
                                sql.append(", ");
                            }
                            count++;
                            sql.append(maybeWrapInQoutes(key));
                        }
                    }
                    sql.append(")");
                    sql.append(" FROM stdin DELIMITER '");
                    sql.append(COPY_COMMAND_SEPARATOR);
                    sql.append("';");
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql.toString());
                    }
                    numberInserted = copyManager.copyIn(sql.toString(), is);
                    try (PreparedStatement preparedStatement = con.prepareStatement("SELECT CURRVAL('\"" + schemaTable.getSchema() + "\".\"" + SchemaManager.VERTEX_PREFIX + schemaTable.getTable() + "_ID_seq\"');")) {
                        ResultSet resultSet = preparedStatement.executeQuery();
                        resultSet.next();
                        endHigh = resultSet.getLong(1);
                        resultSet.close();
                    }
                    //set the id on the vertex
                    long id = endHigh - numberInserted + 1;
                    for (SqlgVertex sqlgVertex : vertices.getRight().keySet()) {
                        sqlgVertex.setInternalPrimaryKey(RecordId.from(schemaTable, id++));
                    }
                }
                verticesRanges.put(schemaTable, Pair.of(endHigh - numberInserted + 1, endHigh));
            }
            return verticesRanges;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flushEdgeCache(SqlgGraph sqlgGraph, Map<SchemaTable, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> edgeCache) {
        C3P0ProxyConnection con = (C3P0ProxyConnection) sqlgGraph.tx().getConnection();
        try {
            Method m = BaseConnection.class.getMethod("getCopyAPI", new Class[]{});
            Object[] arg = new Object[]{};
            CopyManager copyManager = (CopyManager) con.rawConnectionOperation(m, C3P0ProxyConnection.RAW_CONNECTION, arg);

            for (SchemaTable schemaTable : edgeCache.keySet()) {
                Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> triples = edgeCache.get(schemaTable);
                try (InputStream is = mapToEdge_InputStream(triples)) {
                    StringBuffer sql = new StringBuffer();
                    sql.append("COPY ");
                    sql.append(maybeWrapInQoutes(schemaTable.getSchema()));
                    sql.append(".");
                    sql.append(maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + schemaTable.getTable()));
                    sql.append(" (");
                    for (Triple<SqlgVertex, SqlgVertex, Map<String, Object>> triple : triples.values()) {
                        int count = 1;
                        sql.append(maybeWrapInQoutes(triple.getLeft().getSchema() + "." + triple.getLeft().getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
                        sql.append(", ");
                        sql.append(maybeWrapInQoutes(triple.getMiddle().getSchema() + "." + triple.getMiddle().getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
                        for (String key : triple.getRight().keySet()) {
                            if (count <= triple.getRight().size()) {
                                sql.append(", ");
                            }
                            count++;
                            sql.append(maybeWrapInQoutes(key));
                        }
                        break;
                    }
                    sql.append(") ");
                    sql.append(" FROM stdin DELIMITER '");
                    sql.append(COPY_COMMAND_SEPARATOR);
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
    public void flushVertexLabelCache(SqlgGraph sqlgGraph, Map<SqlgVertex, Pair<String, String>> vertexOutInLabelMap) {
        if (!vertexOutInLabelMap.isEmpty()) {
            Connection conn = sqlgGraph.tx().getConnection();
            StringBuilder sql = new StringBuilder();
            sql.append("UPDATE \"VERTICES\" a\n" +
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

    @Override
    public void flushVertexPropertyCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> schemaVertexPropertyCache) {

        Connection conn = sqlgGraph.tx().getConnection();
        for (SchemaTable schemaTable : schemaVertexPropertyCache.keySet()) {

            Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>> vertexKeysPropertyCache = schemaVertexPropertyCache.get(schemaTable);
            SortedSet<String> keys = vertexKeysPropertyCache.getLeft();
            Map<SqlgVertex, Map<String, Object>> vertexPropertyCache = vertexKeysPropertyCache.getRight();


            StringBuilder sql = new StringBuilder();
            sql.append("UPDATE ");
            sql.append(maybeWrapInQoutes(schemaTable.getSchema()));
            sql.append(".");
            sql.append(maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + schemaTable.getTable()));
            sql.append(" a \nSET\n\t(");
            int count = 1;
            for (String key : keys) {
                sql.append(maybeWrapInQoutes(key));
                if (count++ < keys.size()) {
                    sql.append(", ");
                }
            }
            sql.append(") = \n\t(");
            count = 1;
            for (String key : keys) {
                sql.append("v.");
                sql.append(maybeWrapInQoutes(key));
                if (count++ < keys.size()) {
                    sql.append(", ");
                }
            }
            sql.append(")\nFROM (\nVALUES\n\t");
            count = 1;
            for (SqlgVertex sqlgVertex : vertexPropertyCache.keySet()) {
                Map<String, Object> properties = vertexPropertyCache.get(sqlgVertex);
                sql.append("(");
                sql.append(((RecordId) sqlgVertex.id()).getId());
                sql.append(", ");
                int countProperties = 1;
                for (String key : keys) {
                    Object value = properties.get(key);
                    if (value != null) {
                        PropertyType propertyType = PropertyType.from(value);
                        switch (propertyType) {
                            case BOOLEAN:
                                sql.append(value);
                                break;
                            case BYTE:
                                sql.append(value);
                                break;
                            case SHORT:
                                sql.append(value);
                                break;
                            case INTEGER:
                                sql.append(value);
                                break;
                            case LONG:
                                sql.append(value);
                                break;
                            case FLOAT:
                                sql.append(value);
                                break;
                            case DOUBLE:
                                sql.append(value);
                                break;
                            case STRING:
                                //Postgres supports custom quoted strings using the 'with token' clause
                                sql.append("$token$");
                                sql.append(value);
                                sql.append("$token$");
                                break;
                            case BOOLEAN_ARRAY:
                                break;
                            case BYTE_ARRAY:
                                break;
                            case SHORT_ARRAY:
                                break;
                            case INTEGER_ARRAY:
                                break;
                            case LONG_ARRAY:
                                break;
                            case FLOAT_ARRAY:
                                break;
                            case DOUBLE_ARRAY:
                                break;
                            case STRING_ARRAY:
                                break;
                            default:
                                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
                        }
                    } else {
                        //set it to what it is
                        if (sqlgVertex.property(key).isPresent()) {
                            sql.append("$token$");
                            sql.append((Object) sqlgVertex.value(key));
                            sql.append("$token$");

                        } else {
                            sql.append("null");
                        }
                    }
                    if (countProperties++ < keys.size()) {
                        sql.append(", ");
                    }
                }
                sql.append(")");
                if (count++ < vertexPropertyCache.size()) {
                    sql.append(",\n\t");
                }
            }
            sql.append("\n) AS v(id, ");
            count = 1;
            for (String key : keys) {
                sql.append(maybeWrapInQoutes(key));
                if (count++ < keys.size()) {
                    sql.append(", ");
                }
            }
            sql.append(")");
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

    @Override
    public void flushEdgePropertyCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Map<String, Object>>>> edgePropertyCache) {

    }

    @Override
    public void flushRemovedVertices(SqlgGraph sqlgGraph, Map<SchemaTable, List<SqlgVertex>> removeVertexCache) {

        if (!removeVertexCache.isEmpty()) {


            //split the list of vertices, postgres has a 2 byte limit in the in clause
            for (Map.Entry<SchemaTable, List<SqlgVertex>> schemaVertices : removeVertexCache.entrySet()) {

                SchemaTable schemaTable = schemaVertices.getKey();

                Pair<Set<SchemaTable>, Set<SchemaTable>> tableLabels = sqlgGraph.getSchemaManager().getTableLabels(SchemaTable.of(schemaTable.getSchema(), SchemaManager.VERTEX_PREFIX + schemaTable.getTable()));

                dropForeignKeys(sqlgGraph, schemaTable);

                List<SqlgVertex> vertices = schemaVertices.getValue();
                int numberOfLoops = (vertices.size() / PARAMETER_LIMIT);
                int previous = 0;
                for (int i = 1; i <= numberOfLoops + 1; i++) {

                    int subListTo = i * PARAMETER_LIMIT;
                    List<SqlgVertex> subVertices;
                    if (i <= numberOfLoops) {
                        subVertices = vertices.subList(previous, subListTo);
                    } else {
                        subVertices = vertices.subList(previous, vertices.size());
                    }

                    previous = subListTo;

                    if (!subVertices.isEmpty()) {

                        Set<SchemaTable> inLabels = tableLabels.getLeft();
                        Set<SchemaTable> outLabels = tableLabels.getRight();

                        deleteEdges(sqlgGraph, schemaTable, subVertices, inLabels, true);
                        deleteEdges(sqlgGraph, schemaTable, subVertices, outLabels, false);

//                        Pair<Set<Long>, Set<SchemaTable>> outLabels = Pair.of(new HashSet<>(), new HashSet<>());
//                        Pair<Set<Long>, Set<SchemaTable>> inLabels = Pair.of(new HashSet<>(), new HashSet<>());
                        //get all the in and out labels for each vertex
                        //then for all in and out edges
                        //then remove the edges
//                        getInAndOutEdgesToRemove(sqlgGraph, subVertices, outLabels, inLabels);
//                        deleteEdges(sqlgGraph, schemaTable, outLabels, true);
//                        deleteEdges(sqlgGraph, schemaTable, inLabels, false);

                        StringBuilder sql = new StringBuilder("DELETE FROM ");
                        sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
                        sql.append(".");
                        sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes((SchemaManager.VERTEX_PREFIX) + schemaTable.getTable()));
                        sql.append(" WHERE ");
                        sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
                        sql.append(" in (");
                        int count = 1;
                        for (SqlgVertex sqlgVertex : subVertices) {
                            sql.append("?");
                            if (count++ < subVertices.size()) {
                                sql.append(",");
                            }
                        }
                        sql.append(")");
                        if (sqlgGraph.getSqlDialect().needsSemicolon()) {
                            sql.append(";");
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug(sql.toString());
                        }
                        Connection conn = sqlgGraph.tx().getConnection();
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                            count = 1;
                            for (SqlgVertex sqlgVertex : subVertices) {
                                preparedStatement.setLong(count++, ((RecordId) sqlgVertex.id()).getId());
                            }
                            preparedStatement.executeUpdate();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }

//                        sql = new StringBuilder("DELETE FROM ");
//                        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(sqlgGraph.getSqlDialect().getPublicSchema()));
//                        sql.append(".");
//                        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
//                        sql.append(" WHERE ");
//                        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
//                        sql.append(" in (");
//
//                        count = 1;
//                        for (SqlgVertex vertex : subVertices) {
//                            sql.append("?");
//                            if (count++ < subVertices.size()) {
//                                sql.append(",");
//                            }
//                        }
//                        sql.append(")");
//                        if (sqlgGraph.getSqlDialect().needsSemicolon()) {
//                            sql.append(";");
//                        }
//                        if (logger.isDebugEnabled()) {
//                            logger.debug(sql.toString());
//                        }
//                        conn = sqlgGraph.tx().getConnection();
//                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
//                            count = 1;
//                            for (SqlgVertex vertex : subVertices) {
//                                preparedStatement.setLong(count++, (Long) vertex.id());
//                            }
//                            preparedStatement.executeUpdate();
//                        } catch (SQLException e) {
//                            throw new RuntimeException(e);
//                        }
                    }

                }

                createForeignKeys(sqlgGraph, schemaTable);

            }
        }
    }


    private void dropForeignKeys(SqlgGraph sqlgGraph, SchemaTable schemaTable) {

        SchemaManager schemaManager = sqlgGraph.getSchemaManager();
        Map<String, Set<String>> edgeForeignKeys = schemaManager.getEdgeForeignKeys();

        for (Map.Entry<String, Set<String>> edgeForeignKey : edgeForeignKeys.entrySet()) {
            String edgeTable = edgeForeignKey.getKey();
            Set<String> foreignKeys = edgeForeignKey.getValue();
            String[] schemaTableArray = edgeTable.split("\\.");

            for (String foreignKey : foreignKeys) {
                if (foreignKey.startsWith(schemaTable.toString() + "_")) {

                    Set<String> foreignKeyNames = getForeignKeyConstraintNames(sqlgGraph, schemaTableArray[0], schemaTableArray[1]);
                    for (String foreignKeyName : foreignKeyNames) {

                        StringBuilder sql = new StringBuilder();
                        sql.append("ALTER TABLE ");
                        sql.append(maybeWrapInQoutes(schemaTableArray[0]));
                        sql.append(".");
                        sql.append(maybeWrapInQoutes(schemaTableArray[1]));
                        sql.append(" DROP CONSTRAINT ");
                        sql.append(maybeWrapInQoutes(foreignKeyName));
                        if (needsSemicolon()) {
                            sql.append(";");
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug(sql.toString());
                        }
                        Connection conn = sqlgGraph.tx().getConnection();
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                            preparedStatement.executeUpdate();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }

                    }
                }
            }
        }
    }

    private void createForeignKeys(SqlgGraph sqlgGraph, SchemaTable schemaTable) {
        SchemaManager schemaManager = sqlgGraph.getSchemaManager();
        Map<String, Set<String>> edgeForeignKeys = schemaManager.getEdgeForeignKeys();

        for (Map.Entry<String, Set<String>> edgeForeignKey : edgeForeignKeys.entrySet()) {
            String edgeTable = edgeForeignKey.getKey();
            Set<String> foreignKeys = edgeForeignKey.getValue();
            for (String foreignKey : foreignKeys) {
                if (foreignKey.startsWith(schemaTable.toString() + "_")) {
                    String[] schemaTableArray = edgeTable.split("\\.");
                    StringBuilder sql = new StringBuilder();
                    sql.append("ALTER TABLE ");
                    sql.append(maybeWrapInQoutes(schemaTableArray[0]));
                    sql.append(".");
                    sql.append(maybeWrapInQoutes(schemaTableArray[1]));
                    sql.append(" ADD FOREIGN KEY (");
                    sql.append(maybeWrapInQoutes(foreignKey));
                    sql.append(") REFERENCES ");
                    sql.append(maybeWrapInQoutes(schemaTable.getSchema()));
                    sql.append(".");
                    sql.append(maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + schemaTable.getTable()));
                    sql.append(" MATCH SIMPLE");
                    if (needsSemicolon()) {
                        sql.append(";");
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql.toString());
                    }
                    Connection conn = sqlgGraph.tx().getConnection();
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                        preparedStatement.executeUpdate();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private void deleteEdges(SqlgGraph sqlgGraph, SchemaTable schemaTable, List<SqlgVertex> subVertices, Set<SchemaTable> labels, boolean inDirection) {
        for (SchemaTable inLabel : labels) {

            StringBuilder sql = new StringBuilder();
            sql.append("DELETE FROM ");
            sql.append(maybeWrapInQoutes(inLabel.getSchema()));
            sql.append(".");
            sql.append(maybeWrapInQoutes(inLabel.getTable()));
            sql.append(" WHERE ");
            sql.append(maybeWrapInQoutes(schemaTable.toString() + (inDirection ? SchemaManager.IN_VERTEX_COLUMN_END : SchemaManager.OUT_VERTEX_COLUMN_END)));
            sql.append(" IN (");
            int count = 1;
            for (Vertex vertexToDelete : subVertices) {
                sql.append("?");
                if (count++ < subVertices.size()) {
                    sql.append(",");
                }
            }
            sql.append(")");
            if (sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            Connection conn = sqlgGraph.tx().getConnection();
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                count = 1;
                for (Vertex vertexToDelete : subVertices) {
                    preparedStatement.setLong(count++, ((RecordId) vertexToDelete.id()).getId());
                }
                int deleted = preparedStatement.executeUpdate();
                if (logger.isDebugEnabled()) {
                    logger.debug("Deleted " + deleted + " edges from " + inLabel.toString());
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void flushRemovedEdges(SqlgGraph sqlgGraph, Map<SchemaTable, List<SqlgEdge>> removeEdgeCache) {

        if (!removeEdgeCache.isEmpty()) {

            //split the list of edges, postgres has a 2 byte limit in the in clause
            for (Map.Entry<SchemaTable, List<SqlgEdge>> schemaEdges : removeEdgeCache.entrySet()) {

                List<SqlgEdge> edges = schemaEdges.getValue();
                int numberOfLoops = (edges.size() / PARAMETER_LIMIT);
                int previous = 0;
                for (int i = 1; i <= numberOfLoops + 1; i++) {

                    List<SqlgEdge> flattenedEdges = new ArrayList<>();
                    int subListTo = i * PARAMETER_LIMIT;
                    List<SqlgEdge> subEdges;
                    if (i <= numberOfLoops) {
                        subEdges = edges.subList(previous, subListTo);
                    } else {
                        subEdges = edges.subList(previous, edges.size());
                    }
                    previous = subListTo;

                    for (SchemaTable schemaTable : removeEdgeCache.keySet()) {
                        StringBuilder sql = new StringBuilder("DELETE FROM ");
                        sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
                        sql.append(".");
                        sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes((SchemaManager.EDGE_PREFIX) + schemaTable.getTable()));
                        sql.append(" WHERE ");
                        sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
                        sql.append(" in (");
                        int count = 1;
                        for (SqlgEdge sqlgEdge : subEdges) {
                            flattenedEdges.add(sqlgEdge);
                            sql.append("?");
                            if (count++ < subEdges.size()) {
                                sql.append(",");
                            }
                        }
                        sql.append(")");
                        if (sqlgGraph.getSqlDialect().needsSemicolon()) {
                            sql.append(";");
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug(sql.toString());
                        }
                        Connection conn = sqlgGraph.tx().getConnection();
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                            count = 1;
                            for (SqlgEdge sqlgEdge : subEdges) {
                                preparedStatement.setLong(count++, ((RecordId) sqlgEdge.id()).getId());
                            }
                            preparedStatement.executeUpdate();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }

//                    StringBuilder sql = new StringBuilder("DELETE FROM ");
//                    sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(sqlgGraph.getSqlDialect().getPublicSchema()));
//                    sql.append(".");
//                    sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGES));
//                    sql.append(" WHERE ");
//                    sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
//                    sql.append(" in (");
//
//                    int count = 1;
//                    for (SqlgEdge edge : flattenedEdges) {
//                        sql.append("?");
//                        if (count++ < flattenedEdges.size()) {
//                            sql.append(",");
//                        }
//                    }
//                    sql.append(")");
//                    if (sqlgGraph.getSqlDialect().needsSemicolon()) {
//                        sql.append(";");
//                    }
//                    if (logger.isDebugEnabled()) {
//                        logger.debug(sql.toString());
//                    }
//                    Connection conn = sqlgGraph.tx().getConnection();
//                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
//                        count = 1;
//                        for (SqlgEdge edge : flattenedEdges) {
//                            preparedStatement.setLong(count++, (Long) edge.id());
//                        }
//                        preparedStatement.executeUpdate();
//                    } catch (SQLException e) {
//                        throw new RuntimeException(e);
//                    }
                }
            }
        }

    }

    @Override
    public String getBatchNull() {
        return BATCH_NULL;
    }

    private InputStream mapToEdge_InputStream(Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> edgeCache) {
//        Long start = endHigh - edgeCache.size() + 1;
        StringBuilder sb = new StringBuilder();
        int count = 1;
        for (Triple<SqlgVertex, SqlgVertex, Map<String, Object>> triple : edgeCache.values()) {
            sb.append(((RecordId) triple.getLeft().id()).getId());
            sb.append(COPY_COMMAND_SEPARATOR);
            sb.append(((RecordId) triple.getMiddle().id()).getId());
            if (!triple.getRight().isEmpty()) {
                sb.append(COPY_COMMAND_SEPARATOR);
            }
            int countKeys = 1;
            for (String key : triple.getRight().keySet()) {
                Object value = triple.getRight().get(key);
                sb.append(value.toString());
                if (countKeys < triple.getRight().size()) {
                    sb.append(COPY_COMMAND_SEPARATOR);
                }
                countKeys++;
            }
            if (count++ < edgeCache.size()) {
                sb.append("\n");
            }
        }
        return new ByteArrayInputStream(sb.toString().getBytes());
    }

    private InputStream mapToLabeledVertex_InputStream(Pair<SortedSet<String>, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>> vertexCache) {
        //String str = "2,peter\n3,john";
        StringBuilder sb = new StringBuilder();
        int count = 1;
        for (SqlgVertex sqlgVertex : vertexCache.getRight().keySet()) {
            Triple<String, String, Map<String, Object>> triple = vertexCache.getRight().get(sqlgVertex);
            //set the internal batch id to be used with inserting batch edges
            if (!vertexCache.getLeft().isEmpty()) {
                int countKeys = 1;
                for (String key : vertexCache.getLeft()) {
                    if (countKeys > 1 && countKeys <= vertexCache.getLeft().size()) {
                        sb.append(COPY_COMMAND_SEPARATOR);
                    }
                    countKeys++;
                    Object value = triple.getRight().get(key);
                    if (value == null) {
                        sb.append(getBatchNull());
                    } else {
                        sb.append(value.toString());
                    }
                }
            } else {
                sb.append("0");
            }
            if (count++ < vertexCache.getRight().size()) {
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
        if (key instanceof String && ((String)key).length() > 63) {
            validateColumnName((String)key);
        }
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

    @Override
    public boolean needForeignKeyIndex() {
        return true;
    }

    public Set<String> getForeignKeyConstraintNames(SqlgGraph sqlgGraph, String foreignKeySchema, String foreignKeyTable) {
        Set<String> result = new HashSet<>();
        Connection conn = sqlgGraph.tx().getConnection();
        DatabaseMetaData metadata;
        try {
            metadata = conn.getMetaData();
            String childCatalog = null;
            String childSchemaPattern = foreignKeySchema;
            String childTableNamePattern = foreignKeyTable;
            ResultSet resultSet = metadata.getImportedKeys(childCatalog, childSchemaPattern, childTableNamePattern);
            while (resultSet.next()) {
                result.add(resultSet.getString("FK_NAME"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public boolean supportsClientInfo() {
        return true;
    }

    public void validateSchemaName(String schema) {
        if (schema.length() > getMinimumSchemaNameLength()) {
            throw SqlgExceptions.invalidSchemaName("Postgresql schema names can only be 63 characters. " + schema + " exceeds that");
        }
    }

    public void validateTableName(String table) {
        if (table.length() > getMinimumTableNameLength()) {
            throw SqlgExceptions.invalidTableName("Postgresql table names can only be 63 characters. " + table + " exceeds that");
        }
    }

    @Override
    public void validateColumnName(String column) {
        super.validateColumnName(column);
        if (column.length() > getMinimumColumnNameLength()) {
            throw SqlgExceptions.invalidColumnName("Postgresql column names can only be 63 characters. " + column + " exceeds that");
        }
    }

    public int getMinimumSchemaNameLength() {
        return 63;
    }

    public int getMinimumTableNameLength() {
        return 63;
    }

    public int getMinimumColumnNameLength() {
        return 63;
    }
}
