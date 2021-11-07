package org.umlg.sqlg.test.repeatstep;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/06/05
 */
public class NetAggregateTest extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            configuration.setProperty("cache.vertices", true);
            if (!configuration.containsKey("jdbc.url")) {
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
            }
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void before() {
        this.sqlgGraph = SqlgGraph.open(configuration);
    }

    @Test
    public void test2() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Transmission").toList();
        List<Path> vertices = this.sqlgGraph.traversal()
                .V()
                .hasLabel("Transmission")
                .repeat(__.both("link").simplePath())
                .until(
                        __.or(
                                __.has("type", P.within("HubSite", "ASwitch", "BSwitch")),
                                __.has("excluded", true),
                                __.loops().is(12)
                        )
                )
                .path()
                .toList();
        System.out.println(vertices.size());
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }
}
