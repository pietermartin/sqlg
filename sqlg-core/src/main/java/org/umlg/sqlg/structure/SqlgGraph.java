package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.lang.reflect.Constructor;
import java.sql.*;
import java.util.*;

/**
 * Date: 2014/07/12
 * Time: 5:38 AM
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
public class SqlgGraph implements Graph, Graph.Iterators {

    private Logger logger = LoggerFactory.getLogger(SqlgGraph.class.getName());
    private final SqlgTransaction sqlgTransaction;
    private SchemaManager schemaManager;
    private GremlinParser gremlinParser;
    private SqlDialect sqlDialect;
    private String jdbcUrl;
    private ObjectMapper mapper = new ObjectMapper();
    private boolean implementForeignKeys;
    private Configuration configuration = new BaseConfiguration();

    public static <G extends Graph> G open(final Configuration configuration) {
        if (null == configuration) throw Graph.Exceptions.argumentCanNotBeNull("configuration");

        if (!configuration.containsKey("jdbc.url"))
            throw new IllegalArgumentException(String.format("SqlgGraph configuration requires that the %s be set", "jdbc.url"));

        return (G) new SqlgGraph(configuration);
    }

    public static <G extends Graph> G open(final String pathToSqlgProperties) {
        if (null == pathToSqlgProperties) throw Graph.Exceptions.argumentCanNotBeNull("pathToSqlgProperties");

        Configuration configuration;
        try {
            configuration = new PropertiesConfiguration(pathToSqlgProperties);
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
        return open(configuration);
    }

    private SqlgGraph(final Configuration configuration) {
        try {
            Class<?> sqlDialectClass = findSqlGDialect();
            logger.debug(String.format("Initializing Sqlg with %s dialect", sqlDialectClass.getSimpleName()));
            Constructor<?> constructor = sqlDialectClass.getConstructor(Configuration.class);
            this.sqlDialect = (SqlDialect) constructor.newInstance(configuration);
            this.implementForeignKeys = configuration.getBoolean("implementForeignKeys", true);
            this.configuration = configuration;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            this.jdbcUrl = configuration.getString("jdbc.url");
            SqlgDataSource.INSTANCE.setupDataSource(
                    sqlDialect.getJdbcDriver(),
                    configuration.getString("jdbc.url"),
                    configuration.getString("jdbc.username"),
                    configuration.getString("jdbc.password"));
            this.sqlDialect.prepareDB(SqlgDataSource.INSTANCE.get(configuration.getString("jdbc.url")).getConnection());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.sqlgTransaction = new SqlgTransaction(this);
        this.tx().readWrite();
        this.schemaManager = new SchemaManager(this, sqlDialect, configuration);
        this.schemaManager.loadSchema();
        this.gremlinParser = new GremlinParser(this);
        if (!this.sqlDialect.supportSchemas() && !this.schemaManager.schemaExist(this.sqlDialect.getPublicSchema())) {
            //This is for mariadb. Need to make sure a db called public exist
            this.schemaManager.createSchema(this.sqlDialect.getPublicSchema());
        }
        this.schemaManager.ensureGlobalVerticesTableExist();
        this.schemaManager.ensureGlobalEdgesTableExist();
        this.tx().commit();
    }

    Configuration getConfiguration() {
        return configuration;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public GremlinParser getGremlinParser() {
        return gremlinParser;
    }

    public SqlDialect getSqlDialect() {
        return sqlDialect;
    }

    @Override
    public Iterators iterators() {
        return this;
    }

    @Override
    public Configuration configuration() {
        return this.configuration;
    }

    public Vertex addVertex(String label, Map<String, Object> keyValues) {
        Map<Object, Object> tmp = new HashMap<>(keyValues);
        tmp.put(T.label, label);
        return addVertex(SqlgUtil.mapTokeyValues(tmp));
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        int i = 0;
        Object key = null;
        Object value;
        for (Object keyValue : keyValues) {
            if (i++ % 2 == 0) {
                key = keyValue;
            } else {
                value = keyValue;
                if (!key.equals(T.label)) {
                    ElementHelper.validateProperty((String) key, value);
                    this.sqlDialect.validateProperty(key, value);
                }

            }
        }
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        SchemaTable schemaTablePair = SqlgUtil.parseLabel(label, this.getSqlDialect().getPublicSchema());
        this.tx().readWrite();
        this.schemaManager.ensureVertexTableExist(schemaTablePair.getSchema(), schemaTablePair.getTable(), keyValues);
        final SqlgVertex vertex = new SqlgVertex(this, schemaTablePair.getSchema(), schemaTablePair.getTable(), keyValues);
        return vertex;
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        this.tx().readWrite();
        final SqlgGraphTraversal traversal = new SqlgGraphTraversal<Object, Vertex>();
        traversal.addStep(new SqlgGraphStep(traversal, Vertex.class, this, vertexIds));
        traversal.sideEffects().setGraph(this);
        return traversal;
    }

    @Override
    public GraphTraversal<Edge, Edge> E(final Object... edgeIds) {
        this.tx().readWrite();
        final SqlgGraphTraversal traversal = new SqlgGraphTraversal<Object, Vertex>();
        traversal.addStep(new SqlgGraphStep(traversal, Edge.class, this, edgeIds));
        traversal.sideEffects().setGraph(this);
        return traversal;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Object... vertexIds) {
        this.tx().readWrite();
        return _vertices(vertexIds).iterator();
    }

    @Override
    public Iterator<Edge> edgeIterator(final Object... edgeIds) {
        this.tx().readWrite();
        return _edges(edgeIds).iterator();
    }

    @Override
    public <S> GraphTraversal<S, S> of() {
        final SqlgGraphTraversal traversal = new SqlgGraphTraversal<Object, Vertex>();
        traversal.addStep(new StartStep<>(traversal));
        traversal.sideEffects().setGraph(this);
        return traversal;
    }

    public Vertex v(final Object id) {
        GraphTraversal<Vertex, Vertex> t = this.V(id);
        return t.hasNext() ? t.next() : null;
    }

    public Edge e(final Object id) {
        GraphTraversal<Edge, Edge> t = this.E(id);
        return t.hasNext() ? t.next() : null;
    }

    @Override
    public GraphComputer compute(final Class... graphComputerClass) {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public SqlgTransaction tx() {
        return this.sqlgTransaction;
    }

    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    @Override
    public void close() throws Exception {
        if (this.tx().isOpen())
            this.tx().close();
        this.schemaManager.close();
        SqlgDataSource.INSTANCE.close(this.getJdbcUrl());
    }

    public String toString() {
        return StringFactory.graphString(this, "SqlGraph");
    }

    public ISqlGFeatures features() {
        return new SqlGFeatures();
    }


    public interface ISqlGFeatures extends Features {
        boolean supportsBatchMode();
    }

    public class SqlGFeatures implements ISqlGFeatures {
        @Override
        public GraphFeatures graph() {
            return new GraphFeatures() {
                @Override
                public boolean supportsComputer() {
                    return false;
                }

                @Override
                public VariableFeatures variables() {
                    return new SqlVariableFeatures();
                }

                @Override
                public boolean supportsThreadedTransactions() {
                    return false;
                }

            };
        }

        @Override
        public VertexFeatures vertex() {
            return new SqlVertexFeatures();
        }

        @Override
        public EdgeFeatures edge() {
            return new SqlEdgeFeatures();
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }

        @Override
        public boolean supportsBatchMode() {
            return getSqlDialect().supportsBatchMode();
        }

        public class SqlVertexFeatures implements VertexFeatures {

            /**
             * Determines if a {@link Vertex} can support multiple properties with the same key.
             */
            @FeatureDescriptor(name = FEATURE_MULTI_PROPERTIES)
            public boolean supportsMultiProperties() {
                return false;
            }

            /**
             * Determines if a {@link Vertex} can support properties on vertex properties.  It is assumed that a
             * graph will support all the same data types for meta-properties that are supported for regular
             * properties.
             */
            @FeatureDescriptor(name = FEATURE_META_PROPERTIES)
            public boolean supportsMetaProperties() {
                return false;
            }

            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public VertexPropertyFeatures properties() {
                return new SqlGVertexPropertyFeatures();
            }
        }

        public class SqlEdgeFeatures implements EdgeFeatures {
            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public EdgePropertyFeatures properties() {
                return new SqlEdgePropertyFeatures();
            }
        }

        public class SqlGVertexPropertyFeatures implements VertexPropertyFeatures {
            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }

            @Override
            public boolean supportsByteValues() {
                return false;
            }

            @Override
            public boolean supportsFloatValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsFloatValues();
            }

            @Override
            public boolean supportsBooleanArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsBooleanArrayValues();
            }

            @Override
            public boolean supportsByteArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsByteArrayValues();
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsDoubleArrayValues();
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsFloatArrayValues();
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsIntegerArrayValues();
            }

            @Override
            public boolean supportsLongArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsLongArrayValues();
            }

            @Override
            public boolean supportsStringArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsStringArrayValues();
            }

        }

        public class SqlEdgePropertyFeatures implements EdgePropertyFeatures {
            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }

            @Override
            public boolean supportsByteValues() {
                return false;
            }

            @Override
            public boolean supportsFloatValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsFloatValues();
            }

            @Override
            public boolean supportsBooleanArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsBooleanArrayValues();
            }

            @Override
            public boolean supportsByteArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsByteArrayValues();
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsDoubleArrayValues();
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsFloatArrayValues();
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsIntegerArrayValues();
            }

            @Override
            public boolean supportsLongArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsLongArrayValues();
            }

            @Override
            public boolean supportsStringArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsStringArrayValues();
            }
        }

        public class SqlVariableFeatures implements VariableFeatures {
            @Override
            public boolean supportsBooleanValues() {
                return false;
            }

            @Override
            public boolean supportsDoubleValues() {
                return false;
            }

            @Override
            public boolean supportsFloatValues() {
                return false;
            }

            @Override
            public boolean supportsIntegerValues() {
                return false;
            }

            @Override
            public boolean supportsLongValues() {
                return false;
            }

            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsByteValues() {
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
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsStringValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }
        }

    }

    /**
     * This is executes a sql query and returns the result as a json string.
     *
     * @param query The sql to execute.
     * @return The query result as json.
     */
    public String query(String query) {
        try {
            Connection conn = this.tx().getConnection();
            ObjectNode result = this.mapper.createObjectNode();
            ArrayNode dataNode = this.mapper.createArrayNode();
            ArrayNode metaNode = this.mapper.createArrayNode();
            Statement statement = conn.createStatement();
            if (logger.isDebugEnabled()) {
                logger.debug(query);
            }
            ResultSet rs = statement.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            boolean first = true;
            while (rs.next()) {
                int numColumns = rsmd.getColumnCount();
                ObjectNode obj = this.mapper.createObjectNode();
                for (int i = 1; i < numColumns + 1; i++) {
                    String columnName = rsmd.getColumnName(i);
                    Object o = rs.getObject(columnName);
                    int type = rsmd.getColumnType(i);
                    this.sqlDialect.putJsonObject(obj, columnName, type, o);
                    if (first) {
                        this.sqlDialect.putJsonMetaObject(this.mapper, metaNode, columnName, type, o);
                    }
                }
                first = false;
                dataNode.add(obj);
            }
            result.put("data", dataNode);
            result.put("meta", metaNode);
            return result.toString();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            this.tx().rollback();
        }
    }

    /**
     * Executes a the given sql sql and returns the result as vertices.
     * @param sql The sql to execute.
     * @return A List of Vertex
     */
    public List<Vertex> vertexQuery(String sql) {
        List<Vertex> sqlgVertices = new ArrayList<>();
        try {
            Connection conn = this.tx().getConnection();
            Statement statement = conn.createStatement();
            sql = this.sqlDialect.wrapVertexQuery(sql);
            if (logger.isDebugEnabled()) {
                logger.debug(sql);
            }
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                long id = resultSet.getLong("ID");
                String schema = resultSet.getString(SchemaManager.VERTEX_SCHEMA);
                String table = resultSet.getString(SchemaManager.VERTEX_TABLE);
                SqlgVertex sqlgVertex = SqlgVertex.of(this, id, schema, table);
                loadVertexAndLabels(resultSet, sqlgVertex);
                sqlgVertices.add(sqlgVertex);
            }
            return sqlgVertices;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    //indexing
    public void createUniqueConstraint(String label, String propertyKey) {
        this.tx().readWrite();
//        this.getSchemaManager().createUniqueConstraint(label, propertyKey);
    }

    public void createVertexLabeledIndex(String label, Object... dummykeyValues) {
        int i = 0;
        String key = "";
        Object value;
        for (Object keyValue : dummykeyValues) {
            if (i++ % 2 == 0) {
                key = (String) keyValue;
            } else {
                value = keyValue;
                if (!key.equals(T.label)) {
                    ElementHelper.validateProperty(key, value);
                    this.sqlDialect.validateProperty(key, value);
                }

            }
        }
        this.tx().readWrite();
        SchemaTable schemaTablePair = SqlgUtil.parseLabel(label, this.getSqlDialect().getPublicSchema());
        this.getSchemaManager().createVertexIndex(schemaTablePair, dummykeyValues);
    }

    public void createEdgeLabeledIndex(String label, Object... dummykeyValues) {
        int i = 0;
        String key = "";
        Object value;
        for (Object keyValue : dummykeyValues) {
            if (i++ % 2 == 0) {
                key = (String) keyValue;
            } else {
                value = keyValue;
                if (!key.equals(T.label)) {
                    ElementHelper.validateProperty(key, value);
                    this.sqlDialect.validateProperty(key, value);
                }

            }
        }
        this.tx().readWrite();
        SchemaTable schemaTablePair = SqlgUtil.parseLabel(label, this.getSqlDialect().getPublicSchema());
        this.getSchemaManager().createEdgeIndex(schemaTablePair, dummykeyValues);
    }

    public long countVertices() {
        this.tx().readWrite();
        Connection conn = this.tx().getConnection();
        StringBuilder sql = new StringBuilder("select count(1) from ");
        sql.append(this.getSqlDialect().maybeWrapInQoutes(this.getSqlDialect().getPublicSchema()));
        sql.append(".");
        sql.append(this.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public long countEdges() {
        this.tx().readWrite();
        Connection conn = this.tx().getConnection();
        StringBuilder sql = new StringBuilder("select count(1) from ");
        sql.append(this.getSqlDialect().maybeWrapInQoutes(this.getSqlDialect().getPublicSchema()));
        sql.append(".");
        sql.append(this.getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGES));
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    boolean isImplementForeignKeys() {
        return implementForeignKeys;
    }

    private Class<?> findSqlGDialect() {
        try {
            return Class.forName("org.umlg.sqlg.sql.dialect.PostgresDialect");
        } catch (ClassNotFoundException e) {
        }
        try {
            return Class.forName("org.umlg.sqlg.sql.dialect.MariaDbDialect");
        } catch (ClassNotFoundException e) {
        }
        try {
            return Class.forName("org.umlg.sqlg.sql.dialect.HsqldbDialect");
        } catch (ClassNotFoundException e) {
        }
        throw new IllegalStateException("No sqlg dialect found!");
    }

    private Iterable<Vertex> _vertices(final Object... vertexId) {
        List<Vertex> sqlGVertexes = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(this.getSqlDialect().maybeWrapInQoutes(this.getSqlDialect().getPublicSchema()));
        sql.append(".");
        sql.append(this.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
        boolean hasIds = vertexId.length > 0;
        boolean validIdExist = false;
        if (hasIds) {
            sql.append(" WHERE ");
            sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
            sql.append(" IN (");
            int count = 1;
            for (Object o : vertexId) {
                if (!(o instanceof Long)) {
                    count++;
                    continue;
                }
                validIdExist = true;
                Long id = (Long) o;
                sql.append(id.toString());
                if (count++ < vertexId.length) {
                    sql.append(",");
                }
            }
            sql.append(")");
        }
        if (!hasIds || (hasIds && validIdExist)) {
            if (this.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            Connection conn = this.tx().getConnection();
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (Statement statement = conn.createStatement()) {
                statement.execute(sql.toString());
                ResultSet resultSet = statement.getResultSet();
                while (resultSet.next()) {
                    long id = resultSet.getLong("ID");
                    String schema = resultSet.getString(SchemaManager.VERTEX_SCHEMA);
                    String table = resultSet.getString(SchemaManager.VERTEX_TABLE);
                    SqlgVertex sqlgVertex = SqlgVertex.of(this, id, schema, table);

                    loadVertexAndLabels(resultSet, sqlgVertex);
                    sqlGVertexes.add(sqlgVertex);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return sqlGVertexes;
    }

    void loadVertexAndLabels(ResultSet resultSet, SqlgVertex sqlgVertex) throws SQLException {
        Set<SchemaTable> labels = new HashSet<>();
        String inCommaSeparatedLabels = resultSet.getString(SchemaManager.VERTEX_IN_LABELS);
        this.getSchemaManager().convertVertexLabelToSet(labels, inCommaSeparatedLabels);
        sqlgVertex.inLabelsForVertex = new HashSet<>();
        sqlgVertex.inLabelsForVertex.addAll(labels);
        String outCommaSeparatedLabels = resultSet.getString(SchemaManager.VERTEX_OUT_LABELS);
        labels.clear();
        this.getSchemaManager().convertVertexLabelToSet(labels, outCommaSeparatedLabels);
        sqlgVertex.outLabelsForVertex = new HashSet<>();
        sqlgVertex.outLabelsForVertex.addAll(labels);
    }

    private Iterable<Edge> _edges(final Object... edgeIds) {
        List<Edge> sqlGEdges = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(this.getSqlDialect().maybeWrapInQoutes(this.getSqlDialect().getPublicSchema()));
        sql.append(".");
        sql.append(this.getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGES));
        boolean hasIds = edgeIds.length > 0;
        boolean validIdExist = false;
        if (hasIds) {
            sql.append("WHERE ");
            sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
            sql.append(" IN (");
            int count = 1;
            for (Object o : edgeIds) {
                if (!(o instanceof Long)) {
                    count++;
                    continue;
                }
                validIdExist = true;
                Long id = (Long) o;
                sql.append(id.toString());
                if (count++ < edgeIds.length) {
                    sql.append(",");
                }
            }
            sql.append(")");
        }
        if (!hasIds || (hasIds && validIdExist)) {
            if (this.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            Connection conn = this.tx().getConnection();
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (Statement statement = conn.createStatement()) {
                statement.execute(sql.toString());
                ResultSet resultSet = statement.getResultSet();
                while (resultSet.next()) {
                    long id = resultSet.getLong(1);
                    String schema = resultSet.getString(2);
                    String table = resultSet.getString(3);
                    SchemaTable schemaTablePair = SchemaTable.of(schema, table);
                    SqlgEdge sqlGEdge = new SqlgEdge(this, id, schemaTablePair.getSchema(), schemaTablePair.getTable());
                    sqlGEdges.add(sqlGEdge);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return sqlGEdges;
    }
}
