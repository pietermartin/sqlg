package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.process.T
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/19
 * Time: 3:23 PM
 */
public class TestByteSupport extends BaseTest {

    @Test
    public void testByte() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "age", (byte)1);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals((byte)1, v.property("age").value());

    }
}
