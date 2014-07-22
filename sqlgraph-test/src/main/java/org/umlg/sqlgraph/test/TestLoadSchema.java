package org.umlg.sqlgraph.test;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlgraph.structure.SqlGraph;

import java.util.Iterator;

/**
 * Date: 2014/07/22
 * Time: 3:27 PM
 */
public class TestLoadSchema extends BaseTest {

    @Test
    public void testLoadSchema() throws Exception {
        this.sqlGraph.addVertex(Element.LABEL, "Person", "aBoolean", true, "aShort", (short) 1,
                "aInteger", 1, "aLong", 1L, "aFloat", 1F, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
        this.sqlGraph.tx().commit();

        this.sqlGraph.close();

        this.sqlGraph = SqlGraph.open(configuration);
        Iterator<Vertex> iter = this.sqlGraph.V().has(Element.LABEL, "Person");
        Assert.assertTrue(iter.hasNext());
        Vertex v = iter.next();
        Assert.assertEquals(true, v.property("aBoolean").value());
        Assert.assertEquals((short) 1, v.property("aShort").value());
        Assert.assertEquals(1, v.property("aInteger").value());
        Assert.assertEquals(1L, v.property("aLong").value());
        Assert.assertEquals(1F, v.property("aFloat").value());
        Assert.assertEquals(1D, v.property("aDouble").value());
        Assert.assertEquals("aaaaaaaaaaaaa", v.property("aString").value());

    }
}
