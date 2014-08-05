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

    @Test
    public void testRemoveByteArrayProperty() {
        Vertex v1 = sqlG.addVertex("name", "marko", "ages", new byte[]{1, 2, 3, 4});
        this.sqlG.tx().commit();
        Assert.assertTrue(v1.property("ages").isPresent());
        v1.property("ages").remove();
        this.sqlG.tx().commit();
        Assert.assertFalse(v1.property("ages").isPresent());
    }

}
