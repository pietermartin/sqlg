package org.umlg.sqlg.test.mod;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/07/26
 * Time: 4:40 PM
 */
public class TestDeletedVertex extends BaseTest {

    @Test(expected = IllegalStateException.class)
    public void testDeletedVertex() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pieter");
        this.sqlgGraph.tx().close();
        v1.remove();
        this.sqlgGraph.tx().commit();
        SqlgVertex sqlGVertex = new SqlgVertex(this.sqlgGraph, (Long)v1.id(), "Person");
        sqlGVertex.property("name");
    }
}
