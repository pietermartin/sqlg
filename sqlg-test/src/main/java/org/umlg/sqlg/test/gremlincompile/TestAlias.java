package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2015/12/14
 * Time: 10:15 PM
 */
public class TestAlias extends BaseTest {

    @Test
    public void testFieldWithDots() {
        this.sqlgGraph.addVertex(T.label, "Person", "test.1", "a");
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("a", vertices.get(0).value("test.1"));
        Assert.assertTrue(vertices.get(0).property("test.1").isPresent());
    }
}
