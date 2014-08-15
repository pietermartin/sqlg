package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.umlg.sqlg.process.step.map.SqlGGraphStep;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.strategy.SqlGGraphStepStrategy;

import java.beans.PropertyVetoException;
import java.sql.*;
import java.util.Map;

/**
 * Date: 2014/07/12
 * Time: 5:38 AM
 */
public class SqlG implements Graph {

    private final SqlGTransaction sqlGTransaction;
    private SchemaManager schemaManager;
    private SqlDialect sqlDialect;
    private String jdbcUrl;
    private ObjectMapper mapper = new ObjectMapper();

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public SqlDialect getSqlDialect() {
        return sqlDialect;
    }

    public static <G extends Graph> G open(final Configuration configuration) {
        if (null == configuration) throw Graph.Exceptions.argumentCanNotBeNull("configuration");

        if (!configuration.containsKey("jdbc.url"))
            throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        if (!configuration.containsKey("sql.dialect"))
            throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "sql.dialect"));

        return (G) new SqlG(configuration);
    }

    private SqlG(final Configuration configuration) {
        try {
            Class<?> sqlDialectClass = Class.forName(configuration.getString("sql.dialect"));
            this.sqlDialect = (SqlDialect) sqlDialectClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            this.jdbcUrl = configuration.getString("jdbc.url");
            SqlGDataSource.INSTANCE.setupDataSource(
                    sqlDialect.getJdbcDriver(),
                    configuration.getString("jdbc.url"),
                    configuration.getString("jdbc.username"),
                    configuration.getString("jdbc.password"));
            this.sqlDialect.prepareDB(SqlGDataSource.INSTANCE.get(configuration.getString("jdbc.url")).getConnection());
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.sqlGTransaction = new SqlGTransaction(this);
        this.tx().readWrite();
        this.schemaManager = new SchemaManager(this, sqlDialect);
        this.schemaManager.loadSchema();
        this.schemaManager.ensureGlobalVerticesTableExist();
        this.schemaManager.ensureGlobalEdgesTableExist();
        this.tx().commit();
    }

    public Vertex addVertex(String label, Map<String, Object> keyValues) {
        keyValues.put(Element.LABEL, label);
        return addVertex(SqlGUtil.mapTokeyValues(keyValues));
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        int i = 0;
        String key = "";
        Object value;
        for (Object keyValue : keyValues) {
            if (i++ % 2 == 0) {
                key = (String) keyValue;
            } else {
                value = keyValue;
                if (!key.equals(Element.LABEL)) {
                    ElementHelper.validateProperty(key, value);
                    this.sqlDialect.validateProperty(key, value);
                }

            }
        }
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        Pair<String, String> schemaTablePair = SqlGUtil.parseLabel(label, this.getSqlDialect().getPublicSchema());
        this.tx().readWrite();
        this.schemaManager.ensureVertexTableExist(schemaTablePair.getLeft(), schemaTablePair.getRight(), keyValues);
        final SqlVertex vertex = new SqlVertex(this, schemaTablePair.getLeft(), schemaTablePair.getRight(), keyValues);
        return vertex;
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V() {
        this.tx().readWrite();
        final GraphTraversal traversal = new DefaultGraphTraversal<Object, Vertex>();
        traversal.strategies().register(SqlGGraphStepStrategy.instance());
        traversal.addStep(new SqlGGraphStep(traversal, Vertex.class, this));
        traversal.memory().set(Key.hide("g"), this);
        return traversal;
    }

    @Override
    public GraphTraversal<Edge, Edge> E() {
        this.tx().readWrite();
        final GraphTraversal traversal = new DefaultGraphTraversal<Object, Edge>();
        traversal.strategies().register(SqlGGraphStepStrategy.instance());
        traversal.addStep(new SqlGGraphStep(traversal, Edge.class, this));
        traversal.memory().set(Key.hide("g"), this);
        return traversal;
    }


    @Override
    public <S> GraphTraversal<S, S> of() {
        final GraphTraversal<S, S> traversal = new DefaultGraphTraversal<>();
        traversal.memory().set(Graph.Key.hide("g"), this);
        traversal.strategies().register(SqlGGraphStepStrategy.instance());
        traversal.addStep(new StartStep<>(traversal));
        return traversal;
    }

    @Override
    public Vertex v(final Object id) {
        this.tx().readWrite();
        if (null == id) throw Graph.Exceptions.elementNotFound(Vertex.class, id);

        SqlVertex sqlVertex = null;
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(this.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
        sql.append(" WHERE ");
        sql.append(this.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" = ?");
        if (this.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            Long idAsLong = evaluateToLong(id);
            preparedStatement.setLong(1, idAsLong);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String schema = resultSet.getString("VERTEX_SCHEMA");
                String table = resultSet.getString("VERTEX_TABLE");
                sqlVertex = new SqlVertex(this, idAsLong, schema, table);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (sqlVertex == null) {
            throw Graph.Exceptions.elementNotFound(Vertex.class, id);
        }
        return sqlVertex;
    }

    @Override
    public Edge e(final Object id) {
        this.tx().readWrite();
        if (null == id) throw Graph.Exceptions.elementNotFound(Edge.class, id);

        SqlEdge sqlEdge = null;
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(this.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGES));
        sql.append(" WHERE ");
        sql.append(this.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" = ?");
        if (this.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            Long idAsLong = evaluateToLong(id);
            preparedStatement.setLong(1, idAsLong);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String schema = resultSet.getString("EDGE_SCHEMA");
                String table = resultSet.getString("EDGE_TABLE");
                sqlEdge = new SqlEdge(this, idAsLong, schema, table);
            }
            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (sqlEdge == null) {
            throw Graph.Exceptions.elementNotFound(Edge.class, id);
        }
        return sqlEdge;

    }

    @Override
    public GraphComputer compute(final Class... graphComputerClass) {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public SqlGTransaction tx() {
        return this.sqlGTransaction;
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
        SqlGDataSource.INSTANCE.close(this.getJdbcUrl());
    }

    public String toString() {
        return StringFactory.graphString(this, "SqlGraph");
    }

    @Override
    public Features getFeatures() {
        return new SqlGFeatures();
    }

    public class SqlGFeatures implements Features {
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

                @Override
                public boolean supportsFullyIsolatedTransactions() {
                    return true;
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

        public class SqlVertexFeatures implements VertexFeatures {

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
            public boolean supportsFloatValues() {
                return SqlG.this.getSchemaManager().getSqlDialect().supportsFloatValues();
            }

            @Override
            public boolean supportsBooleanArrayValues() {
                return SqlG.this.getSchemaManager().getSqlDialect().supportsBooleanArrayValues();
            }

            @Override
            public boolean supportsByteArrayValues() {
                return SqlG.this.getSchemaManager().getSqlDialect().supportsByteArrayValues();
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return SqlG.this.getSchemaManager().getSqlDialect().supportsDoubleArrayValues();
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return SqlG.this.getSchemaManager().getSqlDialect().supportsFloatArrayValues();
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return SqlG.this.getSchemaManager().getSqlDialect().supportsIntegerArrayValues();
            }

            @Override
            public boolean supportsLongArrayValues() {
                return SqlG.this.getSchemaManager().getSqlDialect().supportsLongArrayValues();
            }

            @Override
            public boolean supportsStringArrayValues() {
                return SqlG.this.getSchemaManager().getSqlDialect().supportsStringArrayValues();
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
            public boolean supportsFloatValues() {
                return SqlG.this.getSchemaManager().getSqlDialect().supportsFloatValues();
            }

            @Override
            public boolean supportsBooleanArrayValues() {
                return SqlG.this.getSchemaManager().getSqlDialect().supportsBooleanArrayValues();
            }

            @Override
            public boolean supportsByteArrayValues() {
                return SqlG.this.getSchemaManager().getSqlDialect().supportsByteArrayValues();
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return SqlG.this.getSchemaManager().getSqlDialect().supportsDoubleArrayValues();
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return SqlG.this.getSchemaManager().getSqlDialect().supportsFloatArrayValues();
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return SqlG.this.getSchemaManager().getSqlDialect().supportsIntegerArrayValues();
            }

            @Override
            public boolean supportsLongArrayValues() {
                return SqlG.this.getSchemaManager().getSqlDialect().supportsLongArrayValues();
            }

            @Override
            public boolean supportsStringArrayValues() {
                return SqlG.this.getSchemaManager().getSqlDialect().supportsStringArrayValues();
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

    public String query(String query) {
        try {
            Connection conn = SqlGDataSource.INSTANCE.get(this.getJdbcUrl()).getConnection();
            conn.setReadOnly(true);
            ObjectNode result = this.mapper.createObjectNode();
            ArrayNode dataNode = this.mapper.createArrayNode();
            ArrayNode metaNode = this.mapper.createArrayNode();
            Statement statement = conn.createStatement();
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
            conn.setReadOnly(false);
            return result.toString();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            this.tx().rollback();
        }
    }

    //indexing
    public void createUniqueConstraint(String label, String propertyKey) {
        this.tx().readWrite();
//        this.getSchemaManager().createUniqueConstraint(label, propertyKey);
    }

    public void createLabeledIndex(String label, String propertyKey) {
        this.tx().readWrite();
        this.getSchemaManager().createIndex(label, propertyKey);
    }

    public long countVertices() {
        this.tx().readWrite();
        Connection conn = this.tx().getConnection();
        StringBuilder sql = new StringBuilder("select count(1) from ");
        sql.append(this.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
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
        sql.append(this.getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGES));
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Long evaluateToLong(final Object id) throws NumberFormatException {
        Long longId;
        if (id instanceof Long)
            longId = (Long) id;
        else if (id instanceof Number)
            longId = ((Number) id).longValue();
        else
            longId = Double.valueOf(id.toString()).longValue();
        return longId;
    }

}
