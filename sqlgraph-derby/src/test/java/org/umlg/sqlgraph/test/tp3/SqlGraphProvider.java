package org.umlg.sqlgraph.test.tp3;

import com.tinkerpop.gremlin.AbstractGraphProvider;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.umlg.sqlgraph.structure.SqlGraph;
import org.umlg.sqlgraph.structure.SqlGraphDataSource;
import org.umlg.sqlgraph.test.JDBC;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2014/07/13
 * Time: 5:57 PM
 */
public class SqlGraphProvider extends AbstractGraphProvider {

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName) {
        return new HashMap<String, Object>() {{
            put("gremlin.graph", SqlGraph.class.getName());
            put("jdbc.driver", "org.apache.derby.jdbc.EmbeddedDriver");
            put("jdbc.url", "jdbc:derby:sqlgraph-derby/src/test/resources/derby/" + graphName + "; create = true");
            put("jdbc.username", "");
            put("jdbc.password", "");
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
            SqlGraphDataSource.INSTANCE.setupDataSource(
                    configuration.getString("jdbc.driver"),
                    configuration.getString("jdbc.url"),
                    configuration.getString("jdbc.username"),
                    configuration.getString("jdbc.password"));
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
        try (Connection conn = SqlGraphDataSource.INSTANCE.get(configuration.getString("jdbc.url")).getConnection()) {
            DatabaseMetaData metadata = conn.getMetaData();
            conn.setAutoCommit(false);
            JDBC.dropSchema(metadata, "APP");
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }


}
