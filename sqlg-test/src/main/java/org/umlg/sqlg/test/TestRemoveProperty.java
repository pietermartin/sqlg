package org.umlg.sqlg.test;

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
        Vertex v1 = sqlG.addVertex("name", "marko");
        this.sqlG.tx().commit();
        v1.property("name").remove();
        Assert.assertFalse(v1.property("name").isPresent());
    }

}
