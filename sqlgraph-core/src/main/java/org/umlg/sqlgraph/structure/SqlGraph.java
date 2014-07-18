package org.umlg.sqlgraph.structure;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;
import org.umlg.sqlgraph.process.step.map.SqlGraphStep;
import org.umlg.sqlgraph.sql.dialect.SqlGraphDialect;
import org.umlg.sqlgraph.strategy.SqlGraphStepTraversalStrategy;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Date: 2014/07/12
 * Time: 5:38 AM
 */
public class SqlGraph implements Graph {

    private final SqlGraphTransaction sqlGraphTransaction;
    private SchemaManager schemaManager;
    private String jdbcUrl;

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public static <G extends Graph> G open(final Configuration configuration) {
        if (null == configuration) throw Graph.Exceptions.argumentCanNotBeNull("configuration");

        if (!configuration.containsKey("jdbc.driver"))
            throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.driver"));

        if (!configuration.containsKey("jdbc.url"))
            throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        return (G) new SqlGraph(configuration);
    }

    private SqlGraph(final Configuration configuration) {
        SqlGraphDialect sqlGraphDialect = null;
        try {
            Class.forName(SqlGraphDialect.HSQLDB.getJdbcDriver());
            sqlGraphDialect = SqlGraphDialect.HSQLDB;
        } catch (ClassNotFoundException e) {
        }
        try {
            Class.forName(SqlGraphDialect.POSTGRES.getJdbcDriver());
            sqlGraphDialect = SqlGraphDialect.POSTGRES;
        } catch (ClassNotFoundException e) {
        }
        try {
            Class.forName(SqlGraphDialect.MARIADBDB.getJdbcDriver());
            sqlGraphDialect = SqlGraphDialect.MARIADBDB;
        } catch (ClassNotFoundException e) {
        }
        if (sqlGraphDialect == null) {
            throw new IllegalStateException("Postgres driver " + SqlGraphDialect.POSTGRES.getJdbcDriver() + " or Hsqldb driver " + SqlGraphDialect.HSQLDB.getJdbcDriver() + " must be on the classpath!");
        }
        try {
            this.jdbcUrl = configuration.getString("jdbc.url");
            SqlGraphDataSource.INSTANCE.setupDataSource(
                    sqlGraphDialect.getJdbcDriver(),
                    configuration.getString("jdbc.url"),
                    configuration.getString("jdbc.username"),
                    configuration.getString("jdbc.password"));
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
        this.sqlGraphTransaction = new SqlGraphTransaction(this);
        this.tx().readWrite();
        this.schemaManager = new SchemaManager(this, sqlGraphDialect.getSqlDialect());
        this.schemaManager.ensureGlobalVerticesTableExist();
        this.schemaManager.ensureGlobalEdgesTableExist();
        this.tx().commit();
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
                if (!key.equals(Element.LABEL))
                    ElementHelper.validateProperty(key, value);
            }
        }
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        this.tx().readWrite();
        this.schemaManager.ensureVertexTableExist(label, keyValues);
        final SqlVertex vertex = new SqlVertex(this, label, keyValues);
        return vertex;
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V() {
        this.tx().readWrite();
        final GraphTraversal traversal = new DefaultGraphTraversal<Object, Vertex>();
        traversal.strategies().register(new SqlGraphStepTraversalStrategy());
        traversal.addStep(new SqlGraphStep<>(traversal, Vertex.class, this));
        return traversal;
    }

    @Override
    public GraphTraversal<Edge, Edge> E() {
        this.tx().readWrite();
        final GraphTraversal traversal = new DefaultGraphTraversal<Object, Edge>();
        traversal.strategies().register(new SqlGraphStepTraversalStrategy());
        traversal.addStep(new SqlGraphStep(traversal, Edge.class, this));
        return traversal;
    }

    @Override
    public Vertex v(final Object id) {
        this.tx().readWrite();
        if (null == id) throw Graph.Exceptions.elementNotFound();

        SqlVertex sqlVertex = null;
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(this.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
        sql.append(" WHERE ");
        sql.append(this.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" = ?;");
        Connection conn = this.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(1, (Long) id);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String label = resultSet.getString("VERTEX_TABLE");
                sqlVertex = new SqlVertex(this, (Long) id, label);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (sqlVertex == null) {
            throw Graph.Exceptions.elementNotFound();
        }
        return sqlVertex;
    }

    @Override
    public Edge e(final Object id) {
        this.tx().readWrite();
        if (null == id) throw Graph.Exceptions.elementNotFound();

        SqlEdge sqlEdge = null;
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(this.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGES));
        sql.append(" WHERE ");
        sql.append(this.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" = ?;");
        Connection conn = this.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(1, (Long) id);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String label = resultSet.getString("EDGE_TABLE");
                sqlEdge = new SqlEdge(this, (Long) id, label);
            }
            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (sqlEdge == null) {
            throw Graph.Exceptions.elementNotFound();
        }
        return sqlEdge;

    }

    @Override
    public <C extends GraphComputer> C compute(Class<C>... graphComputerClass) {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public SqlGraphTransaction tx() {
        return this.sqlGraphTransaction;
    }

    @Override
    public <V extends Variables> V variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    @Override
    public void close() throws Exception {
        if (this.tx().isOpen())
            this.tx().close();
        this.schemaManager.close();
        SqlGraphDataSource.INSTANCE.close(this.getJdbcUrl());
    }

    public String toString() {
        return StringFactory.graphString(this, "SqlGraph");
    }

    public Features getFeatures() {
        return new SqlGraphFeatures();
    }

    public class SqlGraphFeatures implements Features {
        @Override
        public GraphFeatures graph() {
            return new GraphFeatures() {
                @Override
                public boolean supportsComputer() {
                    return false;
                }

                @Override
                public VariableFeatures memory() {
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
            public VertexAnnotationFeatures annotations() {
                return new SqlGraphVertexAnnotationFeatures();
            }

            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public VertexPropertyFeatures properties() {
                return new SqlGraphVertexPropertyFeatures();
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

        public class SqlGraphVertexPropertyFeatures implements VertexPropertyFeatures {
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
                return SqlGraph.this.getSchemaManager().getSqlDialect().supportsFloatValues();
            }

            @Override
            public boolean supportsBooleanArrayValues() {
                return SqlGraph.this.getSchemaManager().getSqlDialect().supportsBooleanArrayValues();
            }

            @Override
            public boolean supportsByteArrayValues() {
                return SqlGraph.this.getSchemaManager().getSqlDialect().supportsByteArrayValues();
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return SqlGraph.this.getSchemaManager().getSqlDialect().supportsDoubleArrayValues();
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return SqlGraph.this.getSchemaManager().getSqlDialect().supportsFloatArrayValues();
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return SqlGraph.this.getSchemaManager().getSqlDialect().supportsIntegerArrayValues();
            }

            @Override
            public boolean supportsLongArrayValues() {
                return SqlGraph.this.getSchemaManager().getSqlDialect().supportsLongArrayValues();
            }

            @Override
            public boolean supportsStringArrayValues() {
                return SqlGraph.this.getSchemaManager().getSqlDialect().supportsStringArrayValues();
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
                return SqlGraph.this.getSchemaManager().getSqlDialect().supportsFloatValues();
            }

            @Override
            public boolean supportsBooleanArrayValues() {
                return SqlGraph.this.getSchemaManager().getSqlDialect().supportsBooleanArrayValues();
            }

            @Override
            public boolean supportsByteArrayValues() {
                return SqlGraph.this.getSchemaManager().getSqlDialect().supportsByteArrayValues();
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return SqlGraph.this.getSchemaManager().getSqlDialect().supportsDoubleArrayValues();
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return SqlGraph.this.getSchemaManager().getSqlDialect().supportsFloatArrayValues();
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return SqlGraph.this.getSchemaManager().getSqlDialect().supportsIntegerArrayValues();
            }

            @Override
            public boolean supportsLongArrayValues() {
                return SqlGraph.this.getSchemaManager().getSqlDialect().supportsLongArrayValues();
            }

            @Override
            public boolean supportsStringArrayValues() {
                return SqlGraph.this.getSchemaManager().getSqlDialect().supportsStringArrayValues();
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

        public class SqlGraphVertexAnnotationFeatures implements VertexAnnotationFeatures {
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


}
