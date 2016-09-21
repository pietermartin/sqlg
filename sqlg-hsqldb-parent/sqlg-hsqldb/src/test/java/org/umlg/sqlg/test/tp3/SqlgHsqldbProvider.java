package org.umlg.sqlg.test.tp3;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.umlg.sqlg.HsqldbPlugin;
import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2014/07/13
 * Time: 5:57 PM
 */
public class SqlgHsqldbProvider extends SqlgAbstractGraphProvider {

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        return new HashMap<String, Object>() {{
            put("gremlin.graph", SqlgGraph.class.getName());
            put("jdbc.url", "jdbc:hsqldb:file:src/test/db/" + graphName + "");
            put("jdbc.username", "SA");
            put("jdbc.password", "");
        }};
    }

    @Override
    public SqlgPlugin getSqlgPlugin() {
        return new HsqldbPlugin();
    }
}
