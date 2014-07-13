package org.umlg.sqlgraph.h2database.test;

import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 6:51 PM
 */
public class TestRemoveProperty extends BaseTest {

    @Test
    public void testRemoveProperty() {
        Vertex v1 = sqlGraph.addVertex("name", "marko");
        this.sqlGraph.tx().commit();
        v1.property("name").remove();
        Assert.assertFalse(v1.property("name").isPresent());
    }

}
