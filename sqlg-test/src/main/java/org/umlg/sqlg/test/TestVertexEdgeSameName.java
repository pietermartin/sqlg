package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.process.traversal.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

/**
 * Date: 2014/07/26
 * Time: 1:49 PM
 */
public class TestVertexEdgeSameName extends BaseTest {

    @Test
    public void testVertexEdgeSameNAme() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pieter");
        v1.addEdge("Person", v2);
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void addForeignKey() {
        Vertex v1 =  this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 =  this.sqlgGraph.addVertex(T.label, "Product");
        v1.addEdge("e1", v2);
        this.sqlgGraph.tx().commit();
        Vertex v3 =  this.sqlgGraph.addVertex(T.label, "Company");
        v1.addEdge("e1", v3);
        this.sqlgGraph.tx().commit();
    }
}
