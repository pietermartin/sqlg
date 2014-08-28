package org.umlg.sqlg.test.mod;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/08/28
 * Time: 7:14 AM
 */
public class TestUpdateVertex extends BaseTest {

    @Test
    public void testUpdateVertex() {
        Vertex v = this.sqlG.addVertex(Element.LABEL, "Person", "name", "john");
        Assert.assertEquals("john", v.value("name"));
        v.property("name", "joe");
        Assert.assertEquals("joe", v.value("name"));
        this.sqlG.tx().commit();
        Assert.assertEquals("joe", v.value("name"));
    }

    @Test
    public void testPropertyIsPresent() {

    }
}
