package org.umlg.sqlg.test.edges;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2014/10/04
 * Time: 12:33 PM
 */
public class TestEdgeCache extends BaseTest {

    @Test
    public void testEdgeUncommittedProperties() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Edge e = a1.addEdge("ab", b1, "name", "test");
        this.sqlgGraph.tx().commit();
        e.property("test", "this");
        assertTrue(this.sqlgGraph.traversal().E(e.id()).next().property("name").isPresent());
        assertTrue(this.sqlgGraph.traversal().E(e.id()).next().property("test").isPresent());
        this.sqlgGraph.traversal().E(e.id()).next().property("name1", "test");
        this.sqlgGraph.traversal().E(e.id()).next().property("test1", "test2");
        assertTrue(this.sqlgGraph.traversal().E(e.id()).next().property("name1").isPresent());
        assertTrue(this.sqlgGraph.traversal().E(e.id()).next().property("test1").isPresent());
    }

    @Test
    public void testEdgeCreateEndsUpInVertexEdgeCache() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);
        assertEquals(1, vertexTraversal(v1).out("friend").count().next().intValue());
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testMultipleEdgesFromSameVertex() throws Exception {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "mike");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw");
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "Bike", "name", "ktm");
        v1.addEdge("bts_aaaaaa", v2);
        v1.addEdge("bts_btsalmtos", v4);
        v1.addEdge("bts_btsalm", v3);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph = SqlgGraph.open(configuration)) {
            v1 = sqlgGraph.v(v1.id());
            assertEquals(1, sqlgGraph.traversal().V(v1.id()).out("bts_btsalm").count().next().intValue());
            assertEquals(1, sqlgGraph.traversal().V(v1.id()).out("bts_btsalmtos").count().next().intValue());
        }
    }
}
