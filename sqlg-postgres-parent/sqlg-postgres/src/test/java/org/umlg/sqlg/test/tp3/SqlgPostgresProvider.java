package org.umlg.sqlg.test.tp3;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.umlg.sqlg.PostgresPlugin;
import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2014/07/13
 * Time: 5:57 PM
 */
public class SqlgPostgresProvider extends SqlgAbstractGraphProvider {

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        return new HashMap<String, Object>() {{
            put("gremlin.graph", SqlgGraph.class.getName());
            put("jdbc.url", "jdbc:postgresql://localhost:5432/" + graphName);
            put("jdbc.username", "postgres");
            put("jdbc.password", "postgres");
            put("maxPoolSize", 10);
        }};
    }

    @Override
    public SqlgPlugin getSqlgPlugin() {
        return new PostgresPlugin();
    }

}
