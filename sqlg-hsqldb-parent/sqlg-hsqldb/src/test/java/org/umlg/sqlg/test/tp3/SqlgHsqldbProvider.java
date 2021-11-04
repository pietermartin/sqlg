package org.umlg.sqlg.test.tp3;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.HsqldbPlugin;
import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.structure.SqlgGraph;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2014/07/13
 * Time: 5:57 PM
 */
public class SqlgHsqldbProvider extends SqlgAbstractGraphProvider {

    private final static Logger LOGGER = LoggerFactory.getLogger(SqlgHsqldbProvider.class.getName());

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        LOGGER.info("Hsqldb, Starting test: " + test.getSimpleName() + "." + testMethodName);
        return new HashMap<String, Object>() {{
            put("gremlin.graph", SqlgGraph.class.getName());
            put("jdbc.url", "jdbc:hsqldb:file:src/test/db/" + graphName + "");
            put("jdbc.username", "SA");
            put("jdbc.password", "");
        }};
    }

    @Override
    public Graph openTestGraph(final Configuration config) {
        SqlgGraph sqlgGraph = (SqlgGraph)GraphFactory.open(config);
        try {
            Connection connection = sqlgGraph.tx().getConnection();
            Statement statement = connection.createStatement();
            statement.execute("SET DATABASE SQL AVG SCALE 2");
            sqlgGraph.tx().commit();
            return sqlgGraph;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SqlgPlugin getSqlgPlugin() {
        return new HsqldbPlugin();
    }
}
