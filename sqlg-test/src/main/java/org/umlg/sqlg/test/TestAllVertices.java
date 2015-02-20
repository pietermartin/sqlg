package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.process.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 3:38 PM
 */
public class TestAllVertices extends BaseTest {

    @Test
    public void testAllVertices()  {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");

        Vertex washineMachine = this.sqlgGraph.addVertex(T.label, "Product", "productName", "Washing Machine");
        marko.addEdge("happiness", washineMachine, "love", true);


        this.sqlgGraph.tx().commit();
        Assert.assertEquals(4L, this.sqlgGraph.V().count().next(), 0);
    }
}
