package org.umlg.sqlg.test.gremlincompile;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

/**
 * Created by pieter on 2015/11/23.
 */
public class TestBulkWithinPerformance extends BaseTest {

    @Test
    public void testBulkWithinPerformance() {
//        this.sqlgGraph.tx().streamingMode();
        List<String> uids = new ArrayList<>();
        for (int i = 0; i < 1_000_000; i++) {
            String uid = UUID.randomUUID().toString();
            uids.add(uid);
            this.sqlgGraph.addVertex(T.label, "Person", "uid", uid);
        }
        this.sqlgGraph.tx().commit();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<String> smallUidSet = new ArrayList<>();
        int count = 2;
        for (int i = 0; i < count; i++) {
            smallUidSet.add(uids.get(i));
        }
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("uid", P.within(smallUidSet)).toList();
        Assert.assertEquals(count, vertices.size());
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch = new StopWatch();
        stopWatch.start();
        smallUidSet = new ArrayList<>();
        count = 3;
        for (int i = 0; i < count; i++) {
            smallUidSet.add(uids.get(i));
        }
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("uid", P.within(smallUidSet)).toList();
        Assert.assertEquals(count, vertices.size());
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }
}
