package org.umlg.sqlg.test.tp3;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.MysqlPlugin;
import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Vasili Pispanen
 * Date: 2018/05/22
 */
public class SqlgMysqlProvider extends SqlgAbstractGraphProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlgMysqlProvider.class);

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        LOGGER.info("Mysql, Starting test: {}.{}", test.getSimpleName(), testMethodName);
        return new HashMap<>() {{
            put("gremlin.graph", SqlgGraph.class.getName());
            put("jdbc.url", "jdbc:mysql://localhost:" + mysqlPort(graphName) + "/?allowPublicKeyRetrieval=true&useSSL=false");
            put("jdbc.username", "root");
            put("jdbc.password", "mysql");
            put("maxPoolSize", 10);
        }};
    }


    @Override
    public SqlgPlugin getSqlgPlugin() {
        return new MysqlPlugin();
    }

    private int mysqlPort(String graphName) {
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
