package org.umlg.sqlgraph.test;

import com.tinkerpop.gremlin.structure.Element;
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
        Vertex marko = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "marko");
        Vertex john = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "john");
        Vertex peter = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "peter");
        this.sqlGraph.tx().commit();
        Assert.assertEquals(3L, this.sqlGraph.V().count().next(), 0);
    }
}
