package org.umlg.sqlg.test.schema;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlG;
import org.umlg.sqlg.test.BaseTest;

import java.util.Iterator;

/**
 * Date: 2014/07/22
 * Time: 3:27 PM
 */
public class TestLoadSchema extends BaseTest {

    @Test
    public void testLoadPropertyColumnNames() throws Exception {
        this.sqlG.addVertex(Element.LABEL, "Person", "name", "a");
        this.sqlG.tx().commit();
        this.sqlG.close();
        this.sqlG = SqlG.open(configuration);
        Vertex v = this.sqlG.V().next();
        v.property("surname", "b");
        this.sqlG.tx().rollback();
        v = this.sqlG.V().next();
        v.property("surname", "b");
        this.sqlG.tx().commit();
    }

    @Test
    public void testLoadSchemaWithByteArray() throws Exception {
        this.sqlG.addVertex(Element.LABEL, "Person", "byteArray", new byte[]{1,2,3,4});
        this.sqlG.tx().commit();
        this.sqlG.close();
        this.sqlG = SqlG.open(configuration);
        Iterator<Vertex> iter = this.sqlG.V().has(Element.LABEL, "Person");
        Assert.assertTrue(iter.hasNext());
        Vertex v = iter.next();
        Assert.assertArrayEquals(new byte[]{1,2,3,4}, v.<byte[]>property("byteArray").value());
    }

    @Test
    public void testLoadSchema() throws Exception {
        this.sqlG.addVertex(Element.LABEL, "Person", "aBoolean", true, "aShort", (short) 1,
                "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
        this.sqlG.tx().commit();
        this.sqlG.close();
        this.sqlG = SqlG.open(configuration);
        Iterator<Vertex> iter = this.sqlG.V().has(Element.LABEL, "Person");
        Assert.assertTrue(iter.hasNext());
        Vertex v = iter.next();
        Assert.assertEquals(true, v.property("aBoolean").value());
        Assert.assertEquals((short) 1, v.property("aShort").value());
        Assert.assertEquals(1, v.property("aInteger").value());
        Assert.assertEquals(1D, v.property("aDouble").value());
        Assert.assertEquals("aaaaaaaaaaaaa", v.property("aString").value());
    }

    @Test
    public void testLoadMultipleSchemas() throws Exception {
        this.sqlG.addVertex(Element.LABEL, "Test1.Person", "aBoolean", true, "aShort", (short) 1,
                "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
        this.sqlG.addVertex(Element.LABEL, "Test2.Person", "aBoolean", true, "aShort", (short) 1,
                "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
        this.sqlG.tx().commit();
        this.sqlG.close();
        this.sqlG = SqlG.open(configuration);
        this.sqlG.addVertex(Element.LABEL, "Test1.Product", "aBoolean", true, "aShort", (short) 1,
                "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
        this.sqlG.addVertex(Element.LABEL, "Test2.Product", "aBoolean", true, "aShort", (short) 1,
                "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
        this.sqlG.tx().commit();
        Assert.assertEquals(1, this.sqlG.V().has(Element.LABEL, "Test1.Person").count().next(), 0);
        Assert.assertEquals(1, this.sqlG.V().has(Element.LABEL, "Test2.Person").count().next(), 0);
        Assert.assertEquals(1, this.sqlG.V().has(Element.LABEL, "Test1.Product").count().next(), 0);
        Assert.assertEquals(1, this.sqlG.V().has(Element.LABEL, "Test2.Product").count().next(), 0);

    }

    @Test
    public void loadForeignKeys() throws Exception {
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person", "aBoolean", true, "aShort", (short) 1,
                "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person", "bBoolean", true, "bShort", (short) 2,
                "bInteger", 2, "bLong", 2L, "bDouble", 2D, "bString", "bbbbbbbbbbbbb");
        v1.addEdge("edgeTest", v2, "cBoolean", true, "cShort", (short) 3,
                "cInteger", 3, "cLong", 3L, "cDouble", 3D, "cString", "ccccccccccccc");
        this.sqlG.tx().commit();
        this.sqlG.close();
        this.sqlG = SqlG.open(configuration);
        v1 = this.sqlG.v(v1.id());
        Assert.assertNotNull(v1);
        Assert.assertEquals(true, v1.property("aBoolean").value());
        Assert.assertEquals((short) 1, v1.property("aShort").value());
        Assert.assertEquals(1, v1.property("aInteger").value());
        Assert.assertEquals(1L, v1.property("aLong").value());
        Assert.assertEquals(1D, v1.property("aDouble").value());
        Assert.assertEquals("aaaaaaaaaaaaa", v1.property("aString").value());

        v2 = this.sqlG.v(v2.id());
        Assert.assertEquals(true, v2.property("bBoolean").value());
        Assert.assertEquals((short) 2, v2.property("bShort").value());
        Assert.assertEquals(2, v2.property("bInteger").value());
        Assert.assertEquals(2L, v2.property("bLong").value());
        Assert.assertEquals(2D, v2.property("bDouble").value());
        Assert.assertEquals("bbbbbbbbbbbbb", v2.property("bString").value());

        Iterator<Edge> edgeIter = v1.outE("edgeTest");
        Assert.assertTrue(edgeIter.hasNext());
        Edge e = edgeIter.next();
        Assert.assertEquals(true, e.property("cBoolean").value());
        Assert.assertEquals((short) 3, e.property("cShort").value());
        Assert.assertEquals(3, e.property("cInteger").value());
        Assert.assertEquals(3L, e.property("cLong").value());
        Assert.assertEquals(3D, e.property("cDouble").value());
        Assert.assertEquals("ccccccccccccc", e.property("cString").value());

        Vertex v3 = this.sqlG.addVertex(Element.LABEL, "Person", "dBoolean", true, "dShort", (short) 4,
                "dInteger", 4, "dLong", 4L, "bDouble", 4D, "dString", "ddddddddddddd");
        v1.addEdge("edgeTest", v3, "eBoolean", true, "eShort", (short) 3,
                "eInteger", 3, "eLong", 3L, "eDouble", 3D, "eString", "eeeeeeeeeeeee");

    }
}
