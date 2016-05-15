package org.umlg.sqlg.test.memory;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/05/15
 * Time: 8:22 AM
 */
public class TestMemoryUsage extends BaseTest {

    @Test
    public void testMemoryUsage() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 1_000_000; i++) {
            this.sqlgGraph.streamVertex(T.label, "A", "name", "a" + i);
        }
        this.sqlgGraph.tx().commit();
        System.out.println("sleeping 1");
        Thread.sleep(10000);
        System.gc();
        System.out.println("sleeping 2");
        Thread.sleep(10000);
        assertEquals(1_000_000, this.sqlgGraph.traversal().V().count().next(), 0);
        System.out.println("sleeping 3");
        System.gc();
        System.out.println("sleeping 4");
        Thread.sleep(10000);
    }
}
