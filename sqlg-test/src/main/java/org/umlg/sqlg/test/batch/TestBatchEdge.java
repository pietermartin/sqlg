package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2015/12/27
 * Time: 8:21 AM
 */
public class TestBatchEdge extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testBatchEdgeDoesNotLooseProperties() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex personA = this.sqlgGraph.addVertex(T.label, "Person", "name", "A");
        Vertex personB = this.sqlgGraph.addVertex(T.label, "Person", "name", "B");
        Edge e = personA.addEdge("loves", personB);
        e.property("from", "nowish");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(e, this.sqlgGraph.traversal().E(e).next());
        Assert.assertTrue(this.sqlgGraph.traversal().E(e).properties("from").hasNext());
        Assert.assertEquals("nowish", this.sqlgGraph.traversal().E(e).next().<String>value("from"));
    }
}
