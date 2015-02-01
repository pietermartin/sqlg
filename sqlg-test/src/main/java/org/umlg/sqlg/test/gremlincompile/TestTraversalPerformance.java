package org.umlg.sqlg.test.gremlincompile;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2015/02/01
 * Time: 11:48 AM
 */
public class TestTraversalPerformance extends BaseTest {

    @Test
    public void testSpeed() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        for (int i = 0; i < 100; i++) {
            Vertex b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("outB", b);
            for (int j = 0; j < 1000; j++) {
                Vertex c = this.sqlgGraph.addVertex(T.label, "C");
                b.addEdge("outC", c);
                for (int k = 0; k < 100; k++) {
                    Vertex d = this.sqlgGraph.addVertex(T.label, "D");
                    c.addEdge("outD", d);
                }
            }
            this.sqlgGraph.tx().commit();
            this.sqlgGraph.tx().batchModeOn();
            System.out.println("inserted " + i);
        }
        this.sqlgGraph.tx().commit();
        System.out.println("done inserting");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Assert.assertEquals(10000000, a.out().out().out().count().next().intValue());
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }
}
