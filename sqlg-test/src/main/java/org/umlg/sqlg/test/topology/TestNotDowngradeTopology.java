package org.umlg.sqlg.test.topology;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;

import java.net.URL;

public class TestNotDowngradeTopology {

    @Test
    public void testNotDowngradeTopology() throws ConfigurationException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        Configurations configs = new Configurations();
        Configuration configuration = configs.properties(sqlProperties);
        if (!configuration.containsKey("jdbc.url")) {
            throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
        }
        SqlgGraph sqlgGraph = SqlgGraph.open(configuration);
        System.out.println(sqlgGraph);
        sqlgGraph.close();
    }
}
