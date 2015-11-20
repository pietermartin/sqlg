package org.umlg.sqlg.test.vertex;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2015/11/19
 * Time: 6:34 PM
 */
public class TestOtherVertex extends BaseTest {

    @Test
    public void testOtherV() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        Traversal<?, Vertex> other = this.sqlgGraph.traversal().V(a1).outE().otherV();
        Assert.assertTrue(other.hasNext());
        Assert.assertEquals(b1, other.next());
    }
}
