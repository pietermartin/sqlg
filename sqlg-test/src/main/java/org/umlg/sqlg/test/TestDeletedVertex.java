package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlVertex;

/**
 * Date: 2014/07/26
 * Time: 4:40 PM
 */
public class TestDeletedVertex extends BaseTest {

    @Test(expected = IllegalStateException.class)
    public void testDeletedVertex() {
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person", "name", "marko");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person", "name", "pieter");
        this.sqlG.tx().close();
        v1.remove();
        this.sqlG.tx().commit();
        SqlVertex sqlVertex = new SqlVertex(this.sqlG, (Long)v1.id(), "Person");
        sqlVertex.property("name");
    }
}
