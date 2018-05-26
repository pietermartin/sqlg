package org.umlg.sqlg.test.tp3;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.MysqlPlugin;
import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.structure.SqlgGraph;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Vasili Pispanen
 *         Date: 2018/05/22
 */
public class SqlgMysqlProvider extends SqlgAbstractGraphProvider {

    private Logger logger = LoggerFactory.getLogger(SqlgMysqlProvider.class.getName());

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        logger.info("Mysql, Starting test: " + test.getSimpleName() + "." + testMethodName);
        Map<String, Object> m = new HashMap<String, Object>() {{
            put("gremlin.graph", SqlgGraph.class.getName());
            put("jdbc.url", "jdbc:mysql://localhost:3306/?useSSL=false");
            put("jdbc.username", "mysql");
            put("jdbc.password", "mysql");
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
                    m.put(k, v);
                }
            } catch (IOException ioe) {
                LoggerFactory.getLogger(getClass()).error("Cannot read properties from sqlg.properties", ioe.getMessage());
            }

        }
        return m;


    }


    @Override
    public SqlgPlugin getSqlgPlugin() {
        return new MysqlPlugin();
    }

}
