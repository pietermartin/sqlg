package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 5:08 PM
 */
public class TestGetById extends BaseTest {

    @Test
    public void testGetVertexById() {
        Vertex marko = this.sqlG.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlG.addVertex(T.label, "Person", "name", "john");
        Vertex peter = this.sqlG.addVertex(T.label, "Person", "name", "peter");
        this.sqlG.tx().commit();
        Vertex v = this.sqlG.v(marko.id());
        Assert.assertEquals(marko, v);
        v = this.sqlG.v(john.id());
        Assert.assertEquals(john, v);
        v = this.sqlG.v(peter.id());
        Assert.assertEquals(peter, v);
    }

    @Test
    public void testGetEdgeById() {
        Vertex marko = this.sqlG.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlG.addVertex(T.label, "Person", "name", "john");
        Edge friendEdge = marko.addEdge("friend", john);
        Edge famliyEdge = marko.addEdge("family", john);
        this.sqlG.tx().commit();
        Assert.assertEquals(friendEdge, this.sqlG.e(friendEdge.id()));
        Assert.assertEquals(famliyEdge, this.sqlG.e(famliyEdge.id()));
    }
}
