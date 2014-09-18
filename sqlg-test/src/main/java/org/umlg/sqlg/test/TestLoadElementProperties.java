package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 5:23 PM
 */
public class TestLoadElementProperties extends BaseTest {

    @Test
    public void testLoadVertexProperties() {
        Vertex marko = this.sqlG.addVertex(T.label, "Person", "name", "marko");
        this.sqlG.tx().commit();
        marko = this.sqlG.v(marko.id());
        Assert.assertEquals("marko", marko.property("name").value());
    }

    @Test
    public void testLoadEdgeProperties() {
        Vertex marko = this.sqlG.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlG.addVertex(T.label, "Person", "name", "john");
        Edge friend = marko.addEdge("friend", john, "weight", 1);
        this.sqlG.tx().commit();
        Assert.assertEquals(1, this.sqlG.e(friend.id()).property("weight").value());
    }

}
