package org.umlg.sqlg.test.memory;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2016/05/15
 * Time: 8:22 AM
 */
public class TestMemoryUsage extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testMemoryUsage() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 1_000_000; i++) {
            this.sqlgGraph.streamVertex(T.label, "A", "name", "a" + i);
        }
        this.sqlgGraph.tx().commit();
        System.gc();
        Thread.sleep(3000);
        assertEquals(1_000_000, this.sqlgGraph.traversal().V().count().next(), 0);
        System.gc();
        Thread.sleep(3000);
        System.out.println(Runtime.getRuntime().freeMemory());
        assertTrue(Runtime.getRuntime().freeMemory() < 2_000_000_000);
    }
}
