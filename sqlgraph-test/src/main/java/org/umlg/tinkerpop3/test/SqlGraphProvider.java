package org.umlg.tinkerpop3.test;

import com.tinkerpop.gremlin.AbstractGraphProvider;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.strategy.StrategyWrappedGraph;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.umlg.sqlgraph.structure.SqlGraphDataSource;
import org.umlg.sqlgraph.structure.SqlGraph;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
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
            put("jdbc.driver", "org.h2.Driver");
            put("jdbc.url", "jdbc:h2:/home/pieter/Downloads/sqlgraph/src/test/h2database/" + graphName + ";MULTI_THREADED=TRUE;LOCK_TIMEOUT=10000");
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (null != g) {
            if (g.getFeatures().graph().supportsTransactions())
                g.tx().rollback();
            g.close();

            StringBuilder sql = new StringBuilder("DROP ALL OBJECTS;");
            Connection conn = null;
            PreparedStatement preparedStatement = null;
            try {
                SqlGraph sqlGraph;
                if (g instanceof StrategyWrappedGraph) {
                    sqlGraph = (SqlGraph)((StrategyWrappedGraph)g).getBaseGraph();
                } else {
                    sqlGraph = (SqlGraph)g;
                }
                conn = SqlGraphDataSource.INSTANCE.get(sqlGraph.getJdbcUrl()).getConnection();
                preparedStatement = conn.prepareStatement(sql.toString());
                preparedStatement.executeUpdate();
                preparedStatement.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    if (preparedStatement != null)
                        preparedStatement.close();
                    if (conn != null)
                        conn.close();
                } catch (SQLException se2) {
                }
            }
        } else {
            FileUtils.cleanDirectory(Paths.get("src/test/h2database").toFile());
        }
    }

}
