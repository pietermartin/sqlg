package org.umlg.sqlg.test.groovy;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.Map;

public class TestGroovy extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestGroovy.class);

    @Test
    public void testGroovy() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        GroovyShell shell = new GroovyShell();
        Map<String, Object> bindings = new HashMap<>();
        bindings.put("g", this.sqlgGraph.traversal());
        Binding groovyBinding = new Binding(bindings);
        Script script1 = shell.parse("g.V().hasLabel('Person').next()");
        script1.setBinding(groovyBinding);

        StopWatch stopWatch =StopWatch.createStarted();
        for (int i = 0; i < 10; i++) {
            Object v = script1.run();
            Assert.assertEquals(v1, v);
            stopWatch.stop();
            LOGGER.info("time: {}", stopWatch);
            stopWatch.reset();
            stopWatch.start();
        }
        stopWatch.stop();
        LOGGER.debug("time: {}", stopWatch);
    }

}
