package org.umlg.sqlg.test.tp3;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.H2Plugin;
import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Lukas Krejci
 * @since 1.3.0
 */
public class SqlgH2Provider extends SqlgAbstractGraphProvider {

    private Logger logger = LoggerFactory.getLogger(SqlgH2Provider.class.getName());

    @Override
    public SqlgPlugin getSqlgPlugin() {
        return new H2Plugin();
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        logger.info("H2, Starting test: " + test.getSimpleName() + "." + testMethodName);
        return new HashMap<String, Object>() {{
            put("gremlin.graph", SqlgGraph.class.getName());
            put("jdbc.url", "jdbc:h2:file:./src/test/db/" + graphName + "");
            put("jdbc.username", "SA");
            put("jdbc.password", "");
        }};
    }
}
