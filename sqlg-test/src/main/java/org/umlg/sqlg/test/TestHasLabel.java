package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;

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
        Assert.assertEquals(2, this.sqlgGraph.traversal().E().has(T.label, P.within(Collections.singletonList("knows"))).count().next(), 0);
        Assert.assertEquals(2, this.sqlgGraph.traversal().E().has(T.label, P.within(Collections.singletonList("created"))).count().next(), 0);
        Assert.assertEquals(4, this.sqlgGraph.traversal().E().has(T.label, P.within(Arrays.asList("knows", "created"))).count().next(), 0);
    }

    @Test
    public void g_V_outE_hasLabel_inV() {
        loadModern();
        List<Vertex> vertices = gt.V().outE().hasLabel("created").inV().toList();
        Assert.assertEquals(4, vertices.size());
        vertices = gt.V().outE().hasLabel("knows").inV().toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testEdgeHasLabel() {
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "schema1.person");
        Vertex person2 = this.sqlgGraph.addVertex(T.label, "schema1.person");
        Vertex address1 = this.sqlgGraph.addVertex(T.label, "schema2.address");
        Vertex address2 = this.sqlgGraph.addVertex(T.label, "schema2.address");
        person1.addEdge("address", address1);
        person2.addEdge("address", address2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().outE().otherV().count().next().intValue());
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().inE().otherV().count().next().intValue());
    }

    @Test
    public void testEdgeNotHasLabel() {
        loadModern();
        List<Edge> edges = gt.V().outE().not(hasLabel("created").inV()).toList();
        Assert.assertEquals(2, edges.size());
    }

}
