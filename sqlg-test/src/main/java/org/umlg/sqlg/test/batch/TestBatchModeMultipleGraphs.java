package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/11/11
 * Time: 8:54 PM
 */
public class TestBatchModeMultipleGraphs extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDistribution());
    }

    @Test
    public void testStreamingBatchModeOnMultipleGraphs() throws Exception {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStreamingBatchMode());
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "name", "asdasd");
        }
        this.sqlgGraph.tx().flush();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Address", "name", "asdasd");
        }
        this.sqlgGraph.tx().commit();
        Thread.sleep(1000);
        assertEquals(this.sqlgGraph.traversal().V().toList(), this.sqlgGraph1.traversal().V().toList());
    }

    @Test
    public void testNormalBatchModeOnMultipleGraphs() throws Exception {
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 10; i++) {
            Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "asdasd");
            Vertex address = this.sqlgGraph.addVertex(T.label, "Address", "name", "asdasd");
            person.addEdge("address", address, "name", "asdasd");
        }
        this.sqlgGraph.tx().commit();
        Thread.sleep(1000);
        assertEquals(this.sqlgGraph.traversal().V().toList(), this.sqlgGraph1.traversal().V().toList());
    }
}
