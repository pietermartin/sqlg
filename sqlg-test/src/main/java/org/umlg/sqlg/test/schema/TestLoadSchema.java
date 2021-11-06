package org.umlg.sqlg.test.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2014/07/22
 * Time: 3:27 PM
 */
public class TestLoadSchema extends BaseTest {

    @Test
    public void testQueryInTransactionAcrossSchemas() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "test1.Person", "name", "john");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "test2.Car", "model", "vw");
        v1.addEdge("car", v2, "bought", 1);
        List<Vertex> cars = this.sqlgGraph.traversal().V(v1.id()).out("car").toList();
        assertEquals(1, cars.size());
    }

    @Test
    public void testLoadingLocalDateTime() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "createOn", LocalDateTime.now());
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        //noinspection Duplicates
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Vertex vv = sqlgGraph1.traversal().V(v.id()).next();
            Assert.assertTrue(vv.property("createOn").isPresent());
            Map<String, PropertyType> propertyTypeMap = sqlgGraph1.getTopology().getAllTables().get(SchemaTable.of(
                    sqlgGraph1.getSqlDialect().getPublicSchema(), "V_Person").toString());
            Assert.assertTrue(propertyTypeMap.containsKey("createOn"));
            sqlgGraph1.tx().rollback();
        }
    }

    @Test
    public void testLoadingLocalDate() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "createOn", LocalDate.now());
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        //noinspection Duplicates
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Vertex vv = sqlgGraph1.traversal().V(v.id()).next();
            Assert.assertTrue(vv.property("createOn").isPresent());
            Map<String, PropertyType> propertyTypeMap = sqlgGraph1.getTopology().getAllTables().get(SchemaTable.of(
                    sqlgGraph1.getSqlDialect().getPublicSchema(), "V_Person").toString());
            Assert.assertTrue(propertyTypeMap.containsKey("createOn"));
            sqlgGraph1.tx().rollback();
        }
    }

    @Test
    public void testLoadingLocalTime() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "createOn", LocalTime.now());
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        //noinspection Duplicates
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Vertex vv = sqlgGraph1.traversal().V(v.id()).next();
            Assert.assertTrue(vv.property("createOn").isPresent());
            Map<String, PropertyType> propertyTypeMap = sqlgGraph1.getTopology().getAllTables().get(SchemaTable.of(
                    sqlgGraph1.getSqlDialect().getPublicSchema(), "V_Person").toString());
            Assert.assertTrue(propertyTypeMap.containsKey("createOn"));
            sqlgGraph1.tx().rollback();
        }
    }

    @Test
    public void testLoadingJson() {
        ObjectMapper objectMapper =  new ObjectMapper();
        ObjectNode json = new ObjectNode(objectMapper.getNodeFactory());
        json.put("username", "john");
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "doc", json);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Vertex vv = sqlgGraph1.traversal().V(v1.id()).next();
            Assert.assertTrue(vv.property("doc").isPresent());
            Map<String, PropertyType> propertyTypeMap = sqlgGraph1.getTopology().getAllTables().get(SchemaTable.of(
                    sqlgGraph1.getSqlDialect().getPublicSchema(), "V_Person").toString());
            Assert.assertTrue(propertyTypeMap.containsKey("doc"));
            sqlgGraph1.tx().rollback();
        }
    }

    @Test
    public void testIdNotLoadedAsProperty() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Vertex vv = sqlgGraph1.traversal().V(v.id()).next();
            Assert.assertFalse(vv.property("ID").isPresent());
            Map<String, PropertyType> propertyTypeMap = sqlgGraph1.getTopology().getAllTables().get(SchemaTable.of(
                    sqlgGraph1.getSqlDialect().getPublicSchema(), "V_Person").toString());
            Assert.assertFalse(propertyTypeMap.containsKey("ID"));
            sqlgGraph1.tx().rollback();
        }
    }

    @Test
    public void testLoadPropertyColumnNames() {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Vertex v = sqlgGraph1.traversal().V().next();
            v.property("surname", "b");
            sqlgGraph1.tx().rollback();
            v = sqlgGraph1.traversal().V().next();
            v.property("surname", "b");
            sqlgGraph1.tx().commit();
        }
    }

    @Test
    public void testLoadSchemaWithByteArray() {
        this.sqlgGraph.addVertex(T.label, "Person", "byteArray", new byte[]{1,2,3,4});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Iterator<Vertex> iter = sqlgGraph1.traversal().V().has(T.label, "Person");
            Assert.assertTrue(iter.hasNext());
            Vertex v = iter.next();
            Assert.assertArrayEquals(new byte[]{1,2,3,4}, v.<byte[]>property("byteArray").value());
        }
    }

    @Test
    public void testLoadSchema() {
        this.sqlgGraph.addVertex(T.label, "Person", "aBoolean", true, "aShort", (short) 1,
                "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Iterator<Vertex> iter = sqlgGraph1.traversal().V().has(T.label, "Person");
            Assert.assertTrue(iter.hasNext());
            Vertex v = iter.next();
            assertEquals(true, v.property("aBoolean").value());
            assertEquals((short) 1, v.property("aShort").value());
            assertEquals(1, v.property("aInteger").value());
            assertEquals(1D, v.property("aDouble").value());
            assertEquals("aaaaaaaaaaaaa", v.property("aString").value());
        }
    }

    @Test
    public void testLoadMultipleSchemas() {
        this.sqlgGraph.addVertex(T.label, "Test1.Person", "aBoolean", true, "aShort", (short) 1,
                "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
        this.sqlgGraph.addVertex(T.label, "Test2.Person", "aBoolean", true, "aShort", (short) 1,
                "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            sqlgGraph1.addVertex(T.label, "Test1.Product", "aBoolean", true, "aShort", (short) 1,
                    "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
            sqlgGraph1.addVertex(T.label, "Test2.Product", "aBoolean", true, "aShort", (short) 1,
                    "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
            sqlgGraph1.tx().commit();
            assertEquals(1, sqlgGraph1.traversal().V().has(T.label, "Test1.Person").count().next(), 0);
            assertEquals(1, sqlgGraph1.traversal().V().has(T.label, "Test2.Person").count().next(), 0);
            assertEquals(1, sqlgGraph1.traversal().V().has(T.label, "Test1.Product").count().next(), 0);
            assertEquals(1, sqlgGraph1.traversal().V().has(T.label, "Test2.Product").count().next(), 0);
        }
    }

    @Test
    public void loadForeignKeys() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "aBoolean", true, "aShort", (short) 1,
                "aInteger", 1, "aLong", 1L, "aDouble", 1D, "aString", "aaaaaaaaaaaaa");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "bBoolean", true, "bShort", (short) 2,
                "bInteger", 2, "bLong", 2L, "bDouble", 2D, "bString", "bbbbbbbbbbbbb");
        v1.addEdge("edgeTest", v2, "cBoolean", true, "cShort", (short) 3,
                "cInteger", 3, "cLong", 3L, "cDouble", 3D, "cString", "ccccccccccccc");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph = SqlgGraph.open(configuration)) {
            v1 = sqlgGraph.traversal().V(v1.id()).next();
            Assert.assertNotNull(v1);
            assertEquals(true, v1.property("aBoolean").value());
            assertEquals((short) 1, v1.property("aShort").value());
            assertEquals(1, v1.property("aInteger").value());
            assertEquals(1L, v1.property("aLong").value());
            assertEquals(1D, v1.property("aDouble").value());
            assertEquals("aaaaaaaaaaaaa", v1.property("aString").value());

            v2 = sqlgGraph.traversal().V(v2.id()).next();
            assertEquals(true, v2.property("bBoolean").value());
            assertEquals((short) 2, v2.property("bShort").value());
            assertEquals(2, v2.property("bInteger").value());
            assertEquals(2L, v2.property("bLong").value());
            assertEquals(2D, v2.property("bDouble").value());
            assertEquals("bbbbbbbbbbbbb", v2.property("bString").value());

            Iterator<Edge> edgeIter = vertexTraversal(sqlgGraph, v1).outE("edgeTest");
            Assert.assertTrue(edgeIter.hasNext());
            Edge e = edgeIter.next();
            assertEquals(true, e.property("cBoolean").value());
            assertEquals((short) 3, e.property("cShort").value());
            assertEquals(3, e.property("cInteger").value());
            assertEquals(3L, e.property("cLong").value());
            assertEquals(3D, e.property("cDouble").value());
            assertEquals("ccccccccccccc", e.property("cString").value());

            Vertex v3 = sqlgGraph.addVertex(T.label, "Person", "dBoolean", true, "dShort", (short) 4,
                    "dInteger", 4, "dLong", 4L, "bDouble", 4D, "dString", "ddddddddddddd");
            v1.addEdge("edgeTest", v3, "eBoolean", true, "eShort", (short) 3,
                    "eInteger", 3, "eLong", 3L, "eDouble", 3D, "eString", "eeeeeeeeeeeee");
            sqlgGraph.tx().commit();

            try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
                Edge edgeTest = sqlgGraph1.traversal().V(v3).inE("edgeTest").next();
                assertEquals(true, edgeTest.value("eBoolean"));
                assertEquals((short)3, edgeTest.<Short>value("eShort").shortValue());
                assertEquals(3, edgeTest.<Integer>value("eInteger").intValue());
                assertEquals(3L, edgeTest.<Long>value("eLong").longValue());
                assertEquals(3D, edgeTest.value("eDouble"), 0);
                assertEquals("eeeeeeeeeeeee", edgeTest.<String>value("eString"));
            }
        }

    }

    @Test
    public void testLoadSchemaWithSimilarForeignKeysAcrossSchemas() {
        Vertex realBsc = this.sqlgGraph.addVertex(T.label, "real.bsc");
        Vertex realBscWE = this.sqlgGraph.addVertex(T.label, "workspaceElement");
        realBsc.addEdge("workspaceElement", realBscWE);

        Vertex planBsc = this.sqlgGraph.addVertex(T.label, "plan.bsc");
        Vertex planBscWE = this.sqlgGraph.addVertex(T.label, "workspaceElement");
        planBsc.addEdge("workspaceElement", planBscWE);

        this.sqlgGraph.tx().commit();
        assertEquals(1, sqlgGraph.traversal().V(realBscWE.id()).in("workspaceElement").count().next().intValue());
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph = SqlgGraph.open(configuration)) {
            assertEquals(1, sqlgGraph.traversal().V(realBscWE.id()).in("workspaceElement").count().next().intValue());
            assertEquals(2, sqlgGraph.getTopology().getEdgeForeignKeys().get("plan.E_workspaceElement").size());
            assertEquals(2, sqlgGraph.getTopology().getEdgeForeignKeys().get("real.E_workspaceElement").size());
        }
    }

    //TODO https://github.com/pietermartin/sqlg/issues/238
    @Test
    public void testLoadSchemaWithSimilarForeignKeysAcrossSchemasMultipleEdges() {
        Assume.assumeFalse(this.sqlgGraph.getSqlDialect().isMssqlServer());
        Vertex realBsc = this.sqlgGraph.addVertex(T.label, "real.bsc");
        Vertex realBscWE = this.sqlgGraph.addVertex(T.label, "workspaceElement");
        Vertex planBsc = this.sqlgGraph.addVertex(T.label, "plan.bsc");
        realBsc.addEdge("workspaceElement", realBscWE);
        realBsc.addEdge("workspaceElement", planBsc);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph = SqlgGraph.open(configuration)) {
            assertEquals(2, vertexTraversal(sqlgGraph,  sqlgGraph.traversal().V(realBsc.id()).next()).out("workspaceElement").count().next().intValue());
            assertEquals(1, vertexTraversal(sqlgGraph, sqlgGraph.traversal().V(realBscWE.id()).next()).in("workspaceElement").count().next().intValue());
            assertEquals(1, vertexTraversal(sqlgGraph, sqlgGraph.traversal().V(planBsc.id()).next()).in("workspaceElement").count().next().intValue());
        }
    }

    @Test
    public void testLoadSchemaWithSimilarForeignKeysAcrossSchemasMultipleEdgesOtherWayAround() {
        Vertex realBsc = this.sqlgGraph.addVertex(T.label, "real.bsc");
        Vertex realBscWE = this.sqlgGraph.addVertex(T.label, "workspaceElement");
        Vertex planBsc = this.sqlgGraph.addVertex(T.label, "plan.bsc");
        Vertex planBscWE = this.sqlgGraph.addVertex(T.label, "workspaceElement");
        realBscWE.addEdge("workspaceElement", realBsc);
        planBscWE.addEdge("workspaceElement", planBsc);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph = SqlgGraph.open(configuration)) {
            assertEquals(1, vertexTraversal(sqlgGraph, sqlgGraph.traversal().V(realBscWE.id()).next()).out("workspaceElement").count().next().intValue());
            assertEquals(1, vertexTraversal(sqlgGraph, sqlgGraph.traversal().V(planBscWE.id()).next()).out("workspaceElement").count().next().intValue());
        }
    }

    @Test
    public void testSameEdgeToDifferentVertexLabels() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C.C", "name", "c1");
        Object a1Id = a1.id();
        Object b1Id = b1.id();
        a1.addEdge("ab", b1, "weight", 5);
        a1.addEdge("ab", c1, "weight", 6);
        b1.addEdge("ba", a1, "wtf", "wtf1");
        b1.addEdge("ba", c1, "wtf", "wtf1");
        this.sqlgGraph.tx().commit();
        assertEquals(2, this.sqlgGraph.traversal().V(a1Id).out().count().next().intValue());
        assertEquals(2, this.sqlgGraph.traversal().V(b1Id).out().count().next().intValue());
    }

    @Test
    public void testLoadSchemaSameTableDifferentSchema() {
        @SuppressWarnings("unused")
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "test1.Person", "name1", "john");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "test2.Person", "name2", "john");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph = SqlgGraph.open(configuration)) {
            v2 = sqlgGraph.traversal().V(v2.id()).next();
            //This fails if the columns are not loaded per schema and table
            v2.property("name1", "joe");
            sqlgGraph.tx().commit();
        }
    }

    @Test
    public void testMultipleInEdges() {
        Vertex report1 = this.sqlgGraph.addVertex(T.label, "Report", "name", "report1");
        Vertex favouriteReport = this.sqlgGraph.addVertex(T.label, "FavouriteReport", "name", "favourite");
        Vertex policyReport = this.sqlgGraph.addVertex(T.label, "PolicyReport", "name", "policy");
        report1.addEdge("label", favouriteReport);
        report1.addEdge("label", policyReport);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph = SqlgGraph.open(configuration)) {
            Assert.assertTrue(sqlgGraph.traversal().V().hasLabel("Report").hasNext());
            report1 = sqlgGraph.traversal().V().hasLabel("Report").next();
            assertEquals(2, sqlgGraph.traversal().V(report1).out("label").count().next(), 0);
        }
    }

    @Test
    public void testMultipleOutEdges() {
        Vertex report1 = this.sqlgGraph.addVertex(T.label, "Report", "name", "report1");
        Vertex favouriteReport = this.sqlgGraph.addVertex(T.label, "FavouriteReport", "name", "favourite");
        Vertex policyReport = this.sqlgGraph.addVertex(T.label, "PolicyReport", "name", "policy");
        favouriteReport.addEdge("label", report1);
        policyReport.addEdge("label", report1);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph = SqlgGraph.open(configuration)) {
            Assert.assertTrue(sqlgGraph.traversal().V().hasLabel("Report").hasNext());
            report1 = sqlgGraph.traversal().V().hasLabel("Report").next();
            assertEquals(2, sqlgGraph.traversal().V(report1).in("label").count().next(), 0);
        }
    }

    @Test
    public void testMoreMultipleInEdges() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D");
        a1.addEdge("hi", b1);
        a1.addEdge("hi", c1);
        a1.addEdge("hi", d1);
        this.sqlgGraph.tx().commit();
        assertEquals(3, this.sqlgGraph.traversal().V(a1).out().count().next(), 0);
    }

}
