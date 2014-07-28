package org.umlg.sqlgraph.test;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

/**
 * Date: 2014/07/26
 * Time: 1:49 PM
 */
public class TestVertexEdgeSameName extends BaseTest {

    @Test
    public void testVertexEdgeSameNAme() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "marko");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "pieter");
        v1.addEdge("Person", v2);
        this.sqlGraph.tx().commit();
    }

    @Test
    public void addForeignKey() {
        Vertex v1 =  this.sqlGraph.addVertex(Element.LABEL, "Person");
        Vertex v2 =  this.sqlGraph.addVertex(Element.LABEL, "Product");
        v1.addEdge("e1", v2);
        this.sqlGraph.tx().commit();
        Vertex v3 =  this.sqlGraph.addVertex(Element.LABEL, "Company");
        v1.addEdge("e1", v3);
        this.sqlGraph.tx().commit();
    }
}
