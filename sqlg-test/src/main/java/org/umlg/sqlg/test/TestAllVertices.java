package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 3:38 PM
 */
public class TestAllVertices extends BaseTest {

    @Test
    public void testAllVertices()  {
        Vertex marko = this.sqlG.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlG.addVertex(T.label, "Person", "name", "john");
        Vertex peter = this.sqlG.addVertex(T.label, "Person", "name", "peter");

        Vertex washineMachine = this.sqlG.addVertex(T.label, "Product", "productName", "Washing Machine");
        marko.addEdge("happiness", washineMachine, "love", true);


        this.sqlG.tx().commit();
        Assert.assertEquals(4L, this.sqlG.V().count().next(), 0);
    }
}
