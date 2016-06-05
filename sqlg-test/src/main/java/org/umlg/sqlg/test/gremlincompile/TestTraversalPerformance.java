package org.umlg.sqlg.test.gremlincompile;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

import static org.junit.Assert.assertEquals;

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
        assertEquals(10000, vertices.size());
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }

    @Test
    public void testSpeed() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        for (int i = 0; i < 500_000; i++) {
            Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "name_" + i);
            a.addEdge("outB", b);
            for (int j = 0; j < 1; j++) {
                Vertex c = this.sqlgGraph.addVertex(T.label, "C", "name", "name_" + i + " " + j);
                b.addEdge("outC", c);
            }
//            if (i % 100_000 == 0) {
//                this.sqlgGraph.tx().commit();
//                this.sqlgGraph.tx().normalBatchModeOn();
//                System.out.println("inserted " + i);
//            }
        }
        this.sqlgGraph.tx().commit();
        System.out.println("done inserting");
//        Thread.sleep(10000);
        System.out.println("querying");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<Path> paths = vertexTraversal(a).as("a").out().as("b").out().as("c").path().toList();
        int count = 0;
        for (Path path : paths) {
            String s = path.<Vertex>get(0).value("name") + ": " + path.<Vertex>get(1).value("name") + ": " + path.<Vertex>get(2).value("name");
            if (count % 100_000 == 0) {
                System.out.println(s);
            }
            count++;
        }
//        Assert.assertEquals(100_000, vertexTraversal(a).out().out().count().next().intValue());
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }
}
