package org.umlg.sqlgraph.test;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlgraph.structure.SqlVertex;

/**
 * Date: 2014/07/26
 * Time: 4:40 PM
 */
public class TestDeletedVertex extends BaseTest {

    @Test(expected = IllegalStateException.class)
    public void testDeletedVertex() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "marko");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "pieter");
        this.sqlGraph.tx().close();
        v1.remove();
        this.sqlGraph.tx().commit();
        SqlVertex sqlVertex = new SqlVertex(this.sqlGraph, (Long)v1.id(), "Person");
        sqlVertex.property("name");
    }
}
