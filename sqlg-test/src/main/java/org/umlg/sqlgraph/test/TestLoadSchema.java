package org.umlg.sqlgraph.test;

import com.tinkerpop.gremlin.structure.Edge;
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

    @Test
    public void loadForeignKeys() throws Exception {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person", "aBoolean", true, "aShort", (short) 1,
                "aInteger", 1, "aLong", 1L, "aFloat", 1F, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person", "bBoolean", true, "bShort", (short) 2,
                "bInteger", 2, "bLong", 2L, "bFloat", 2F, "bDouble", 2D, "bString", "bbbbbbbbbbbbb");
        v1.addEdge("edgeTest", v2, "cBoolean", true, "cShort", (short) 3,
                "cInteger", 3, "cLong", 3L, "cFloat", 3F, "cDouble", 3D, "cString", "ccccccccccccc");
        this.sqlGraph.tx().commit();
        this.sqlGraph.close();
        this.sqlGraph = SqlGraph.open(configuration);
        Iterator<Vertex> iter = this.sqlGraph.V().has(Element.LABEL, "Person");
        Assert.assertTrue(iter.hasNext());
        v1 = iter.next();
        Assert.assertEquals(true, v1.property("aBoolean").value());
        Assert.assertEquals((short) 1, v1.property("aShort").value());
        Assert.assertEquals(1, v1.property("aInteger").value());
        Assert.assertEquals(1L, v1.property("aLong").value());
        Assert.assertEquals(1F, v1.property("aFloat").value());
        Assert.assertEquals(1D, v1.property("aDouble").value());
        Assert.assertEquals("aaaaaaaaaaaaa", v1.property("aString").value());

        Assert.assertTrue(iter.hasNext());
        v2 = iter.next();
        Assert.assertEquals(true, v2.property("bBoolean").value());
        Assert.assertEquals((short) 2, v2.property("bShort").value());
        Assert.assertEquals(2, v2.property("bInteger").value());
        Assert.assertEquals(2L, v2.property("bLong").value());
        Assert.assertEquals(2F, v2.property("bFloat").value());
        Assert.assertEquals(2D, v2.property("bDouble").value());
        Assert.assertEquals("bbbbbbbbbbbbb", v2.property("bString").value());

        Iterator<Edge> edgeIter = v1.outE("edgeTest");
        Assert.assertTrue(edgeIter.hasNext());
        Edge e = edgeIter.next();
        Assert.assertEquals(true, e.property("cBoolean").value());
        Assert.assertEquals((short) 3, e.property("cShort").value());
        Assert.assertEquals(3, e.property("cInteger").value());
        Assert.assertEquals(3L, e.property("cLong").value());
        Assert.assertEquals(3F, e.property("cFloat").value());
        Assert.assertEquals(3D, e.property("cDouble").value());
        Assert.assertEquals("ccccccccccccc", e.property("cString").value());

        Vertex v3 = this.sqlGraph.addVertex(Element.LABEL, "Person", "dBoolean", true, "dShort", (short) 4,
                "dInteger", 4, "dLong", 4L, "dFloat", 4F, "bDouble", 4D, "dString", "ddddddddddddd");
        v1.addEdge("edgeTest", v3, "eBoolean", true, "eShort", (short) 3,
                "eInteger", 3, "eLong", 3L, "eFloat", 3F, "eDouble", 3D, "eString", "eeeeeeeeeeeee");

    }
}
