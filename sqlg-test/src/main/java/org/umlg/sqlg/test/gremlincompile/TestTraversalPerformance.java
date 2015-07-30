package org.umlg.sqlg.test.gremlincompile;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2015/02/01
 * Time: 11:48 AM
 */
public class TestTraversalPerformance extends BaseTest {

    @Test
    public void test() {
        for (int i = 0; i < 10000; i++) {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b);
        }
        this.sqlgGraph.tx().commit();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").out().toList();
        Assert.assertEquals(10000, vertices.size());
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }

    @Test
    public void testSpeed() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        for (int i = 0; i < 1000000; i++) {
            Vertex b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("outB", b);
            for (int j = 0; j < 1; j++) {
                Vertex c = this.sqlgGraph.addVertex(T.label, "C");
                b.addEdge("outC", c);
            }
            if (i % 10000 == 0) {
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().batchModeOn();
                System.out.println("inserted " + i);
            }
        }
        this.sqlgGraph.tx().commit();
        System.out.println("done inserting");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Assert.assertEquals(1000000, vertexTraversal(a).out().out().count().next().intValue());
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }
}
