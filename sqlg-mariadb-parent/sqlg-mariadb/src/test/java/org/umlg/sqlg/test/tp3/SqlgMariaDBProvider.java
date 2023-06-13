package org.umlg.sqlg.test.tp3;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.plugin.MariadbPlugin;
import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.structure.SqlgGraph;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Date: 2014/07/13
 * Time: 5:57 PM
 */
public class SqlgMariaDBProvider extends SqlgAbstractGraphProvider {

    private final static Logger LOGGER = LoggerFactory.getLogger(SqlgMariaDBProvider.class);

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        LOGGER.info("MariaDB, Starting test: " + test.getSimpleName() + "." + testMethodName);
        Map<String, Object> m = new HashMap<String, Object>() {{
            put("gremlin.graph", SqlgGraph.class.getName());
            put("jdbc.url", "jdbc:mariadb://localhost:3306/?useSSL=false");
            put("jdbc.username", "mariadb");
            put("jdbc.password", "mariadb");
            put("maxPoolSize", 10);
        }};

        InputStream sqlProperties = Thread.currentThread().getContextClassLoader().getResourceAsStream("sqlg.properties");

        if (sqlProperties != null) {
            Properties p = new Properties();
            try {
                p.load(sqlProperties);
                sqlProperties.close();
                for (String k : p.stringPropertyNames()) {
                    String v = p.getProperty(k);
//                    if (k.equals("jdbc.url")) {
//                        v = v.substring(0, v.lastIndexOf("/") + 1);
//                        v = v + graphName;
//                    }
                    m.put(k, v);
                }
            } catch (IOException ioe) {
                LOGGER.error("Cannot read properties from sqlg.properties", ioe);
            }
        }
        return m;
    }


    @Override
    public SqlgPlugin getSqlgPlugin() {
        return new MariadbPlugin();
    }

}
