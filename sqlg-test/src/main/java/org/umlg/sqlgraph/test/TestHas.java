package org.umlg.sqlgraph.test;

import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 6:36 PM
 */
public class TestHas extends BaseTest {

    @Test
    public void testHas() {
        Vertex v1 = sqlGraph.addVertex("name", "marko");
        Vertex v2 = sqlGraph.addVertex("name", "peter");
        this.sqlGraph.tx().commit();
        Assert.assertEquals(1, this.sqlGraph.V().has("name", "marko").count().next(), 0);
    }
}
