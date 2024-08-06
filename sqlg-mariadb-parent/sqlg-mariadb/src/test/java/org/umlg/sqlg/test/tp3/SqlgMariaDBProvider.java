package org.umlg.sqlg.test.tp3;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.plugin.MariadbPlugin;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2014/07/13
 * Time: 5:57 PM
 */
public class SqlgMariaDBProvider extends SqlgAbstractGraphProvider {

    private final static Logger LOGGER = LoggerFactory.getLogger(SqlgMariaDBProvider.class);

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        LOGGER.info("MariaDB, Starting test: " + test.getSimpleName() + "." + testMethodName);
        return new HashMap<>() {{
            put("gremlin.graph", SqlgGraph.class.getName());
            put("jdbc.url", "jdbc:mariadb://localhost:" + mariaDBPort(graphName) + "/?useSSL=false");
            put("jdbc.username", "root");
            put("jdbc.password", "mariadb");
            put("maxPoolSize", 10);
        }};
    }


    @Override
    public SqlgPlugin getSqlgPlugin() {
        return new MariadbPlugin();
    }

    private int mariaDBPort(String graphName) {
        return switch (graphName) {
            case "g1" -> 3311;
            case "g2" -> 3312;
            case "readGraph" -> 3313;
            case "standard" -> 3314;
            case "temp" -> 3315;
            case "temp1" -> 3316;
            case "temp2" -> 3317;
            case "subgraph" -> 3318;
            case "prototype" -> 3319;
            case "target" -> 3320;
            default -> throw new IllegalStateException("Unhandled graphName " + graphName);
        };
    }
}
