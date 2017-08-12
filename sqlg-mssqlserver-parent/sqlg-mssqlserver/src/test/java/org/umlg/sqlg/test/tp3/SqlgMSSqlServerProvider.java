package org.umlg.sqlg.test.tp3;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.mssqlserver.MSSqlServerPlugin;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2014/07/13
 * Time: 5:57 PM
 */
public class SqlgMSSqlServerProvider extends SqlgAbstractGraphProvider {

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        System.out.println(graphName);
        Map<String, Object> m = new HashMap<String, Object>() {{
            put("gremlin.graph", SqlgGraph.class.getName());
            put("jdbc.url", "jdbc:sqlserver://localhost:1433;databaseName=" + graphName);
            put("jdbc.username", "SA");
            put("jdbc.password", "S0utp@nsb3rg");
            put("maxPoolSize", 10);
        }};
        return m;
    }


    @Override
    public SqlgPlugin getSqlgPlugin() {
        return new MSSqlServerPlugin();
    }

}
