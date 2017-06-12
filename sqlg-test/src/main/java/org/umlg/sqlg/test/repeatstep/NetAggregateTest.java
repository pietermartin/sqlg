package org.umlg.sqlg.test.repeatstep;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/06/05
 */
public class NetAggregateTest extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
            configuration.setProperty("cache.vertices", true);
            if (!configuration.containsKey("jdbc.url")) {
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
            }
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test2() {
        List<Path> vertices = this.sqlgGraph.traversal()
                .V()
                .hasLabel("TransmissionAtoB")
                .has("node", "X5069")
                .repeat(__.out().simplePath())
//                .until(__.or(__.loops().is(P.gt(15)), __.has("hubSite", true)))
                .until(__.has("hubSite", true))
                .path()
//                .limit(1000)
                .toList();
        System.out.println(vertices.size());
    }
}
