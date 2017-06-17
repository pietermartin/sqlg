package org.umlg.sqlg.test.schema;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/08/13
 * Time: 10:49 AM
 */
public class TestSchema extends BaseTest {

    @Test
    public void testEscapingEscapeCharacterInNames() {
        String schemaName = "TestDeleteDataSchema\";commit;drop database \"TestSQLInjection";
        this.sqlgGraph.getTopology().ensureSchemaExist(schemaName);
        this.sqlgGraph.tx().commit();
        String tableName = "A\"A";
        this.sqlgGraph.addVertex(T.label, schemaName + "." + tableName);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel(schemaName + "." + tableName).count().next(), 0);
        Assert.assertTrue(this.sqlgGraph.getTopology().getSchema(schemaName).isPresent());
        Assert.assertTrue(this.sqlgGraph.getTopology().getSchema(schemaName).get().getVertexLabel(tableName).isPresent());
    }

//    @Test
//    public void testEdgeAcrossSchemaCreatesPropertyInAll() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C.C");
//        Edge ab1 = a1.addEdge("yourEdge", b1);
//        Edge bc1 = b1.addEdge("yourEdge", c1);
//        this.sqlgGraph.tx().commit();
//        ab1.property("test", "halo");
//        this.sqlgGraph.tx().commit();
//        Assert.assertTrue(this.sqlgGraph.traversal().E(ab1.id()).next().property("test").isPresent());
//        Assert.assertFalse(this.sqlgGraph.traversal().E(bc1.id()).next().property("test").isPresent());
//        bc1.property("test", "halo");
//        this.sqlgGraph.tx().commit();
//        Assert.assertTrue(this.sqlgGraph.traversal().E(bc1.id()).next().property("test").isPresent());
//    }
//
//    @Test
//    public void testEdgesAcrossSchema() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C.C");
//        a1.addEdge("yourEdge", b1);
//        b1.addEdge("yourEdge", c1);
//        this.sqlgGraph.tx().commit();
//        List<Edge> edges = this.sqlgGraph.traversal().E().toList();
//        Assert.assertEquals(2, edges.size());
//        for (Edge edge : edges) {
//            Assert.assertEquals("yourEdge", edge.label());
//        }
//        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("A.yourEdge").count().next(), 0);
//        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("B.yourEdge").count().next(), 0);
//    }
//
//    @Test
//    public void testSchema() {
//        this.sqlgGraph.addVertex(T.label, "TEST_SCHEMA1.Person", "name", "John");
//        this.sqlgGraph.tx().commit();
//        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
//    }
//
//    @Test
//    public void testEdgeBetweenSchemas() {
//        Vertex john = this.sqlgGraph.addVertex(T.label, "TEST_SCHEMA1.Person", "name", "John");
//        Vertex tom = this.sqlgGraph.addVertex(T.label, "TEST_SCHEMA2.Person", "name", "Tom");
//        Vertex ape = this.sqlgGraph.addVertex(T.label, "TEST_SCHEMA2.Ape", "name", "Amuz");
//        john.addEdge("friend", tom);
//        john.addEdge("pet", ape);
//        this.sqlgGraph.tx().commit();
//        Assert.assertEquals(3, this.sqlgGraph.traversal().V().count().next(), 0);
//        Assert.assertEquals(1, vertexTraversal(this.sqlgGraph, john).out("friend").count().next(), 0);
//        Assert.assertEquals(tom, vertexTraversal(this.sqlgGraph, john).out("friend").next());
//        Assert.assertEquals(john, vertexTraversal(this.sqlgGraph, tom).in("friend").next());
//        Assert.assertEquals(2, this.sqlgGraph.traversal().E().count().next(), 0);
//        this.sqlgGraph.traversal().E().<Edge>has(T.label, "friend").forEachRemaining(
//                a -> {
//                    Assert.assertEquals(john, edgeTraversal(this.sqlgGraph, a).outV().next());
//                    Assert.assertEquals(tom, edgeTraversal(this.sqlgGraph, a).inV().next());
//                }
//        );
//        Assert.assertEquals(1, vertexTraversal(this.sqlgGraph, john).out("friend").has("name", "Tom").count().next(), 0);
//        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
//        Assert.assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "TEST_SCHEMA1.Person").count().next(), 0);
//        Assert.assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "TEST_SCHEMA2.Person").count().next(), 0);
//        Assert.assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "TEST_SCHEMA2.Ape").count().next(), 0);
//        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Ape").count().next(), 0);
//    }
//
//    @Test
//    public void testManySchemas() {
//        Vertex previous = null;
//        for (int i = 0; i < 10; i++) {
//            for (int j = 0; j < 100; j++) {
//                Vertex v = this.sqlgGraph.addVertex(T.label, "Schema" + i + ".Person", "name1", "n" + j, "name2", "n" + j);
//                if (previous != null) {
//                    previous.addEdge("edge", v, "name1", "n" + j, "name2", "n" + j);
//                }
//                previous = v;
//            }
//        }
//        this.sqlgGraph.tx().commit();
//        Assert.assertEquals(1000, this.sqlgGraph.traversal().V().count().next(), 0);
//        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
//        Assert.assertEquals(100, this.sqlgGraph.traversal().V().has(T.label, "Schema5.Person").count().next(), 0);
//        Assert.assertEquals(999, this.sqlgGraph.traversal().E().count().next(), 0);
//        // all schemas are now taken into account (see https://github.com/pietermartin/sqlg/issues/65)
//        Assert.assertEquals(999, this.sqlgGraph.traversal().E().has(T.label, "edge").count().next(), 0);
//        Assert.assertEquals(100, this.sqlgGraph.traversal().E().has(T.label, "Schema0.edge").count().next(), 0);
//        Assert.assertEquals(99, this.sqlgGraph.traversal().E().has(T.label, "Schema9.edge").count().next(), 0);
//    }
//
//    @Test
//    public void testLabelsForSchemaBeforeCommit() {
//        this.sqlgGraph.addVertex(T.label, "Person");
//        this.sqlgGraph.addVertex(T.label, "Person");
//        Assert.assertEquals(2, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
//    }
//
//    @Test
//    public void testGetAllTableLabels() {
//        Vertex person = this.sqlgGraph.addVertex(T.label, "Person");
//        Vertex address = this.sqlgGraph.addVertex(T.label, "Address");
//        person.addEdge("person_address", address);
//        this.sqlgGraph.tx().commit();
//
//        Assert.assertTrue(this.sqlgGraph.getTopology().getTableLabels(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person")) != null);
//
//        Pair<Set<SchemaTable>, Set<SchemaTable>> labels = this.sqlgGraph.getTopology().getTableLabels(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person"));
//        Assert.assertTrue(labels.getRight().contains(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "E_person_address")));
//
//        Map<String, Set<String>> edgeForeignKeys = this.sqlgGraph.getTopology().getAllEdgeForeignKeys();
//        Assert.assertTrue(edgeForeignKeys.containsKey(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "E_person_address").toString()));
//
//        Vertex car = this.sqlgGraph.addVertex(T.label, "Car");
//        person.addEdge("drives", car);
//
//        Vertex pet = this.sqlgGraph.addVertex(T.label, "Pet");
//        person.addEdge("person_address", pet);
//
//        labels = this.sqlgGraph.getTopology().getTableLabels(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person"));
//        Assert.assertTrue(labels.getRight().contains(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "E_person_address")));
//
//        edgeForeignKeys = this.sqlgGraph.getTopology().getAllEdgeForeignKeys();
//        Assert.assertTrue(edgeForeignKeys.containsKey(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "E_person_address").toString()));
//
//        this.sqlgGraph.tx().rollback();
//    }
//
//    @Test
//    public void testSchemaPropertyEndingIn_ID() {
//        this.sqlgGraph.addVertex(T.label, "A", "TRX Group ID", 1234);
//        this.sqlgGraph.addVertex(T.label, "A", "TRX Group ID", 1234);
//        this.sqlgGraph.addVertex(T.label, "A", "TRX Group ID", 1234);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
//        Assert.assertEquals(3, vertices.size());
//        Assert.assertTrue(vertices.get(0).property("TRX Group ID").isPresent());
//        Assert.assertTrue(vertices.get(1).property("TRX Group ID").isPresent());
//        Assert.assertTrue(vertices.get(2).property("TRX Group ID").isPresent());
//    }
//
//    @Test
//    public void testUnprefixedEdgeLabel() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B");
//        a1.addEdge("eee", b1);
//        this.sqlgGraph.tx().commit();
//        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("eee").count().next().intValue());
//        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("A.eee").count().next().intValue());
//        Assert.assertEquals(0, this.sqlgGraph.traversal().E().hasLabel("B.eee").count().next().intValue());
//        Assert.assertEquals(0, this.sqlgGraph.traversal().E().hasLabel("public.eee").count().next().intValue());
//    }
//
//    @Test
//    public void testUnprefixedEdgeLabelWithin() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B");
//        Edge eee = a1.addEdge("eee", b1);
//        Edge fff = a1.addEdge("fff", b1);
//        this.sqlgGraph.tx().commit();
//        GraphTraversal<Edge, Edge> traversal = this.sqlgGraph.traversal().E().has(T.label, P.within("eee", "fff"));
//        List<Edge> edges = traversal.toList();
//        Assert.assertEquals(2, edges.size());
//        Assert.assertTrue(edges.contains(eee) && edges.contains(fff));
//    }
//
//    @Test
//    public void testEnsureSchema() {
//        Topology mgr = this.sqlgGraph.getTopology();
//        this.sqlgGraph.addVertex(T.label, "A.A");
//        this.sqlgGraph.tx().commit();
//
//        Assert.assertTrue(mgr.getSchema(this.sqlgGraph.getSqlDialect().getPublicSchema()).isPresent());
//        Assert.assertTrue(mgr.getSchema("A").isPresent());
//        Assert.assertNotNull(mgr.ensureSchemaExist("A"));
//
//        Assert.assertFalse(mgr.getSchema("B").isPresent());
//        Assert.assertNotNull(mgr.ensureSchemaExist("B"));
//        Assert.assertTrue(mgr.getSchema("B").isPresent());
//        this.sqlgGraph.tx().commit();
//        Assert.assertTrue(mgr.getSchema("B").isPresent());
//    }
//
//    @Test
//    public void testExistingSchema() throws SQLException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsTransactionalSchema());
//        String schema = "A";
//        Connection c = this.sqlgGraph.tx().getConnection();
//        try (Statement ps = c.createStatement();) {
//            ps.executeUpdate("create schema " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
//        }
//        c.commit();
//        Topology mgr = this.sqlgGraph.getTopology();
//
//        Assert.assertFalse(mgr.getSchema("A").isPresent());
//
//        this.sqlgGraph.addVertex(T.label, schema + ".A");
//        this.sqlgGraph.tx().commit();
//        Assert.assertTrue(mgr.getSchema("A").isPresent());
//    }
}
