package org.umlg.sqlg.test;

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
        Vertex v1 = sqlG.addVertex("name", "marko");
        Vertex v2 = sqlG.addVertex("name", "peter");
        this.sqlG.tx().commit();
        Assert.assertEquals(1, this.sqlG.V().has("name", "marko").count().next(), 0);
    }
}
