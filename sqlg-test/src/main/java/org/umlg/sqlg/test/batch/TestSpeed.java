package org.umlg.sqlg.test.batch;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2015/06/03
 * Time: 12:48 PM
 */
public class TestSpeed extends BaseTest {

    @Test
    public void testSpeed() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().batchModeOn();
        Map<String, Object> properties = new HashMap<>();
        for (int j = 0; j < 100; j++) {
            properties.put(String.valueOf(j), "aaaaaaaaaa");
        }
        for (int i = 1; i < 1000001; i++) {
            Vertex person = this.sqlgGraph.addVertex("Person", properties);
            for (int j = 0; j < 5; j++) {
                Vertex address = this.sqlgGraph.addVertex("Address", properties);
                person.addEdge("address", address);
            }
            if (i % 10000 == 0) {
                System.out.println(i);
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().batchModeOn();
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }
}
