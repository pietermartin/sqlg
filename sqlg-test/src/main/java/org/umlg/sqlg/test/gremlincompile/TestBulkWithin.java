package org.umlg.sqlg.test.gremlincompile;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Date: 2015/10/07
 * Time: 7:28 PM
 */
public class TestBulkWithin extends BaseTest {

    @Test
    public void testBulkWithin() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().batchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "God");
        List<String> uuids = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String uuid = UUID.randomUUID().toString();
            uuids.add(uuid);
            Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "idNumber", uuid);
            god.addEdge("creator", person);
            if (i < 10) {
                this.sqlgGraph.addVertex(T.label, "UUID", "uuid", uuid);
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        List<Vertex> persons = this.sqlgGraph.traversal().V().hasLabel("God").out().has("idNumber", P.within(uuids.subList(0, 10).toArray())).toList();
        Assert.assertEquals(10, persons.size());
        stopWatch.stop();
        System.out.println(stopWatch.toString());

    }
}
