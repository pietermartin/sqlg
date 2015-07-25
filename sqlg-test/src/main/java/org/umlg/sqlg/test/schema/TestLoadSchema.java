package org.umlg.sqlg.test.schema;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.util.Iterator;
import java.util.Map;

/**
 * Date: 2014/07/22
 * Time: 3:27 PM
 */
public class TestLoadSchema extends BaseTest {

//    @Test
//    public void testIdNotLoadedAsProperty() throws Exception {
//        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.close();
//        this.sqlgGraph = SqlgGraph.open(configuration);
//        Vertex vv = this.sqlgGraph.traversal().V(v.id()).next();
//        Assert.assertFalse(vv.property("ID").isPresent());
//        Map<String, PropertyType> propertyTypeMap = this.sqlgGraph.getSchemaManager().getAllTables().get(SchemaTable.of(
//                this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person").toString());
//        Assert.assertFalse(propertyTypeMap.containsKey("ID"));
//        this.sqlgGraph.tx().rollback();
//    }
//
//    @Test
//    public void testLoadPropertyColumnNames() throws Exception {
//        this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.close();
//        this.sqlgGraph = SqlgGraph.open(configuration);
//        Vertex v = this.sqlgGraph.traversal().V().next();
//        v.property("surname", "b");
//        this.sqlgGraph.tx().rollback();
//        v = this.sqlgGraph.traversal().V().next();
//        v.property("surname", "b");
//        this.sqlgGraph.tx().commit();
//    }
//
//    @Test
//    public void testLoadSchemaWithByteArray() throws Exception {
//        this.sqlgGraph.addVertex(T.label, "Person", "byteArray", new byte[]{1,2,3,4});
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.close();
//        this.sqlgGraph = SqlgGraph.open(configuration);
//        Iterator<Vertex> iter = this.sqlgGraph.traversal().V().has(T.label, "Person");
//        Assert.assertTrue(iter.hasNext());
//        Vertex v = iter.next();
//        Assert.assertArrayEquals(new byte[]{1,2,3,4}, v.<byte[]>property("byteArray").value());
//    }
//
//    @Test
//    public void testLoadSchema() throws Exception {
//        this.sqlgGraph.addVertex(T.label, "Person", "aBoolean", true, "aShort", (short) 1,
//                "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.close();
//        this.sqlgGraph = SqlgGraph.open(configuration);
//        Iterator<Vertex> iter = this.sqlgGraph.traversal().V().has(T.label, "Person");
//        Assert.assertTrue(iter.hasNext());
//        Vertex v = iter.next();
//        Assert.assertEquals(true, v.property("aBoolean").value());
//        Assert.assertEquals((short) 1, v.property("aShort").value());
//        Assert.assertEquals(1, v.property("aInteger").value());
//        Assert.assertEquals(1D, v.property("aDouble").value());
//        Assert.assertEquals("aaaaaaaaaaaaa", v.property("aString").value());
//    }

    @Test
    public void testLoadMultipleSchemas() throws Exception {
        this.sqlgGraph.addVertex(T.label, "Test1.Person", "aBoolean", true, "aShort", (short) 1,
                "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
        this.sqlgGraph.addVertex(T.label, "Test2.Person", "aBoolean", true, "aShort", (short) 1,
                "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        this.sqlgGraph.addVertex(T.label, "Test1.Product", "aBoolean", true, "aShort", (short) 1,
                "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
        this.sqlgGraph.addVertex(T.label, "Test2.Product", "aBoolean", true, "aShort", (short) 1,
                "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "Test1.Person").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "Test2.Person").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "Test1.Product").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "Test2.Product").count().next(), 0);

    }

//    @Test
//    public void loadForeignKeys() throws Exception {
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "aBoolean", true, "aShort", (short) 1,
//                "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "bBoolean", true, "bShort", (short) 2,
//                "bInteger", 2, "bLong", 2L, "bDouble", 2D, "bString", "bbbbbbbbbbbbb");
//        v1.addEdge("edgeTest", v2, "cBoolean", true, "cShort", (short) 3,
//                "cInteger", 3, "cLong", 3L, "cDouble", 3D, "cString", "ccccccccccccc");
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.close();
//        this.sqlgGraph = SqlgGraph.open(configuration);
//        v1 = this.sqlgGraph.v(v1.id());
//        Assert.assertNotNull(v1);
//        Assert.assertEquals(true, v1.property("aBoolean").value());
//        Assert.assertEquals((short) 1, v1.property("aShort").value());
//        Assert.assertEquals(1, v1.property("aInteger").value());
//        Assert.assertEquals(1L, v1.property("aLong").value());
//        Assert.assertEquals(1D, v1.property("aDouble").value());
//        Assert.assertEquals("aaaaaaaaaaaaa", v1.property("aString").value());
//
//        v2 = this.sqlgGraph.v(v2.id());
//        Assert.assertEquals(true, v2.property("bBoolean").value());
//        Assert.assertEquals((short) 2, v2.property("bShort").value());
//        Assert.assertEquals(2, v2.property("bInteger").value());
//        Assert.assertEquals(2L, v2.property("bLong").value());
//        Assert.assertEquals(2D, v2.property("bDouble").value());
//        Assert.assertEquals("bbbbbbbbbbbbb", v2.property("bString").value());
//
//        Iterator<Edge> edgeIter = vertexTraversal(v1).outE("edgeTest");
//        Assert.assertTrue(edgeIter.hasNext());
//        Edge e = edgeIter.next();
//        Assert.assertEquals(true, e.property("cBoolean").value());
//        Assert.assertEquals((short) 3, e.property("cShort").value());
//        Assert.assertEquals(3, e.property("cInteger").value());
//        Assert.assertEquals(3L, e.property("cLong").value());
//        Assert.assertEquals(3D, e.property("cDouble").value());
//        Assert.assertEquals("ccccccccccccc", e.property("cString").value());
//
//        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "dBoolean", true, "dShort", (short) 4,
//                "dInteger", 4, "dLong", 4L, "bDouble", 4D, "dString", "ddddddddddddd");
//        v1.addEdge("edgeTest", v3, "eBoolean", true, "eShort", (short) 3,
//                "eInteger", 3, "eLong", 3L, "eDouble", 3D, "eString", "eeeeeeeeeeeee");
//
//    }
//
//    @Test
//    public void testLoadSchemaWithSimilarForeignKeysAcrossSchemas() throws Exception {
//        Vertex realBsc = this.sqlgGraph.addVertex(T.label, "real.bsc");
//        Vertex realBscWE = this.sqlgGraph.addVertex(T.label, "workspaceElement");
//        realBsc.addEdge("workspaceElement", realBscWE);
//
//        Vertex planBsc = this.sqlgGraph.addVertex(T.label, "plan.bsc");
//        Vertex planBscWE = this.sqlgGraph.addVertex(T.label, "workspaceElement");
//        planBsc.addEdge("workspaceElement", planBscWE);
//
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.close();
//        this.sqlgGraph = SqlgGraph.open(configuration);
//
//        Assert.assertEquals(1, vertexTraversal(this.sqlgGraph.v(realBscWE.id())).in("workspaceElement").count().next().intValue());
//        Assert.assertEquals(2, this.sqlgGraph.getSchemaManager().getEdgeForeignKeys().get("plan.E_workspaceElement").size());
//        Assert.assertEquals(2, this.sqlgGraph.getSchemaManager().getEdgeForeignKeys().get("real.E_workspaceElement").size());
//    }
//
//    @Test
//    public void testLoadSchemaSameTableDifferentSchema() throws Exception {
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "test1.Person", "name1", "john");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "test2.Person", "name2", "john");
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.close();
//        this.sqlgGraph = SqlgGraph.open(configuration);
//        v2 = this.sqlgGraph.v(v2.id());
//        //This fails if the columns are not loaded per schema and table
//        v2.property("name1", "joe");
//        this.sqlgGraph.tx().commit();
//    }

}
