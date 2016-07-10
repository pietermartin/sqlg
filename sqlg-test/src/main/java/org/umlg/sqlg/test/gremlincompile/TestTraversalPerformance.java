package org.umlg.sqlg.test.gremlincompile;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2015/02/01
 * Time: 11:48 AM
 */
public class TestTraversalPerformance extends BaseTest {

    @Test
    public void testSpeed() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        for (int i = 1; i < 5_00_001; i++) {
            Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "name_" + i);
            a.addEdge("outB", b);
            for (int j = 0; j < 1; j++) {
                Vertex c = this.sqlgGraph.addVertex(T.label, "C", "name", "name_" + i + " " + j);
                b.addEdge("outC", c);
            }
            if (i % 1_000_000 == 0) {
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().normalBatchModeOn();
                System.out.println("inserted " + i);
            }
        }
        this.sqlgGraph.tx().commit();
        System.out.println("done inserting");
//        Thread.sleep(10000);
        System.out.println("querying");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        GraphTraversalSource traversalSource = this.sqlgGraph.traversal();
        for (int i = 0; i < 200; i++) {
            GraphTraversal<Vertex, Path> traversal = traversalSource.V(a).as("a").out().as("b").out().as("c").path();
            while (traversal.hasNext()) {
                Path path = traversal.next();
            }
            stopWatch.stop();
            System.out.println(stopWatch.toString());
            stopWatch.reset();
            stopWatch.start();

        }
        stopWatch.stop();
        System.out.println(stopWatch.toString());
//        Assert.assertEquals(100_000, vertexTraversal(a).out().out().count().next().intValue());
    }

}
