package org.umlg.sqlg.test.tp3;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.*;

import java.lang.reflect.Constructor;
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
    public SqlDialect getSqlgDialect(Configuration configuration) {
        try {
            Class<?> sqlDialectClass = Class.forName("org.umlg.sqlg.sql.dialect.HsqldbDialect");
            Constructor<?> constructor = sqlDialectClass.getConstructor(Configuration.class);
            return (SqlDialect) constructor.newInstance(configuration);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
