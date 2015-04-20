package org.umlg.sqlg.test;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.umlg.sqlg.structure.*;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Date: 2014/07/13
 * Time: 5:57 PM
 */
public class SqlGProvider extends AbstractGraphProvider {

    private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
        add(SqlgEdge.class);
        add(SqlgElement.class);
        add(SqlgGraph.class);
        add(SqlgProperty.class);
        add(SqlgVertex.class);
        add(SqlgVertexProperty.class);
        add(DefaultGraphTraversal.class);
    }};

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        return new HashMap<String, Object>() {{
            put("gremlin.graph", SqlgGraph.class.getName());
            put("jdbc.driver", "org.postgresql.xa.PGXADataSource");
            put("jdbc.url", "jdbc:postgresql://localhost:5432/" + graphName);
            put("jdbc.username", "postgres");
            put("jdbc.password", "postgres");
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (null != g) {
            if (g.features().graph().supportsTransactions())
                g.tx().rollback();
            g.close();
        }
        try {
            SqlgDataSource.INSTANCE.setupDataSource(
                    configuration.getString("jdbc.driver"),
                    configuration.getString("jdbc.url"),
                    configuration.getString("jdbc.username"),
                    configuration.getString("jdbc.password"));
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
        StringBuilder sql = new StringBuilder("DROP SCHEMA IF EXISTS PUBLIC CASCADE;");
        try (Connection conn = SqlgDataSource.INSTANCE.get(configuration.getString("jdbc.url")).getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.executeUpdate();
            }
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql = new StringBuilder("CREATE SCHEMA PUBLIC;");
        // CREATE SCHEMA PUBLIC
        try (Connection conn = SqlgDataSource.INSTANCE.get(configuration.getString("jdbc.url")).getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.executeUpdate();
            }
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }


}
