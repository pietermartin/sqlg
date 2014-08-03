package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/19
 * Time: 3:23 PM
 */
public class TestByteSupport extends BaseTest {

    @Test
    public void testByte() {
        Vertex v = this.sqlG.addVertex(Element.LABEL, "Person", "age", (byte)1);
        this.sqlG.tx().commit();
        Assert.assertEquals((byte)1, v.property("age").value());

    }
}
