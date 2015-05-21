package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Date: 2014/07/29
 * Time: 2:21 PM
 */
public class TestHasLabel extends BaseTest {

    @Test
    public void testHasLabel() {
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(8, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
    }

    @Test
    public void testNonExistingLabel() {
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Animal").count().next(), 0);
    }

    @Test
    public void testInLabels() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        Vertex b = this.sqlgGraph.addVertex(T.label, "Person", "name", "b");
        Vertex c = this.sqlgGraph.addVertex(T.label, "Person", "name", "c");
        Vertex d = this.sqlgGraph.addVertex(T.label, "Person", "name", "d");
        a.addEdge("knows", b);
        a.addEdge("created", b);
        a.addEdge("knows", c);
        a.addEdge("created", d);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(4, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
        Assert.assertEquals(2, this.sqlgGraph.traversal().E().has(T.label, P.within(Arrays.asList("knows"))).count().next(), 0);
        Assert.assertEquals(2, this.sqlgGraph.traversal().E().has(T.label, P.within(Arrays.asList("created"))).count().next(), 0);
        Assert.assertEquals(4, this.sqlgGraph.traversal().E().has(T.label, P.within(Arrays.asList("knows", "created"))).count().next(), 0);
    }

}
