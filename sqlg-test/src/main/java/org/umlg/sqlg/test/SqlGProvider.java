package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.AbstractGraphProvider;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.umlg.sqlg.structure.SqlG;
import org.umlg.sqlg.structure.SqlGDataSource;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2014/07/13
 * Time: 5:57 PM
 */
public class SqlGProvider extends AbstractGraphProvider {

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName) {
        return new HashMap<String, Object>() {{
            put("gremlin.graph", SqlG.class.getName());
            put("jdbc.driver", "org.postgresql.xa.PGXADataSource");
            put("jdbc.url", "jdbc:postgresql://localhost:5432/" + graphName);
            put("jdbc.username", "postgres");
            put("jdbc.password", "postgres");
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (null != g) {
            if (g.getFeatures().graph().supportsTransactions())
                g.tx().rollback();
            g.close();
        }
        try {
            SqlGDataSource.INSTANCE.setupDataSource(
                    configuration.getString("jdbc.driver"),
                    configuration.getString("jdbc.url"),
                    configuration.getString("jdbc.username"),
                    configuration.getString("jdbc.password"));
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
        StringBuilder sql = new StringBuilder("DROP SCHEMA IF EXISTS PUBLIC CASCADE;");
        try (Connection conn = SqlGDataSource.INSTANCE.get(configuration.getString("jdbc.url")).getConnection()) {
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
        try (Connection conn = SqlGDataSource.INSTANCE.get(configuration.getString("jdbc.url")).getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.executeUpdate();
            }
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


}
