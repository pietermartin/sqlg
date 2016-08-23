package org.umlg.sqlg.test.topology;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.strategy.TopologyStrategy;
import org.umlg.sqlg.structure.SchemaManager;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

/**
 * Date: 2016/02/06
 * Time: 6:17 PM
 */
public class TestTopologyUpgrade extends BaseTest {

    @Test
    public void testUpgrade() throws Exception {
        //with topology
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "john");
        Object idA1 = a1.id();
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "joe");
        Object idB1 = b1.id();
        a1.addEdge("knows", b1, "name", "hithere");
        this.sqlgGraph.tx().commit();

        //Delete the topology
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute("DROP SCHEMA " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("sqlg_schema")
                    + (this.sqlgGraph.getSqlDialect().needsSchemaDropCascade() ? " CASCADE" : ""));
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        //topology will be recreated
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(2, sqlgGraph1.traversal().V().count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().E().count().next().intValue());
            Assert.assertTrue(sqlgGraph1.traversal().V().hasLabel("A").hasNext());
            Assert.assertTrue(sqlgGraph1.traversal().V().hasLabel("B").hasNext());
            Assert.assertEquals(1, sqlgGraph1.traversal().V().hasLabel("A").count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().V().hasLabel("B").count().next().intValue());
            Vertex a = sqlgGraph1.traversal().V().hasLabel("A").next();
            Assert.assertEquals(idA1, a.id());
            Vertex b = sqlgGraph1.traversal().V().hasLabel("B").next();
            Assert.assertEquals(idB1, b.id());
            Assert.assertEquals(1, sqlgGraph1.traversal().V(a).out("knows").count().next().intValue());
            Assert.assertEquals(b, sqlgGraph1.traversal().V(a).out("knows").next());
            Assert.assertEquals(1, sqlgGraph1.traversal().V(b).in("knows").count().next().intValue());
            Assert.assertEquals(a, sqlgGraph1.traversal().V(b).in("knows").next());
            Assert.assertEquals(1, sqlgGraph1.traversal().V(a).properties("name").count().next().intValue());
            Assert.assertTrue(sqlgGraph1.traversal().V(a).properties("name").next().isPresent());
            Assert.assertEquals("john", sqlgGraph1.traversal().V(a).properties("name").next().value());
            Assert.assertEquals(1, sqlgGraph1.traversal().V(a).outE("knows").properties("name").count().next().intValue());
            Assert.assertTrue(sqlgGraph1.traversal().V(a).outE("knows").properties("name").next().isPresent());
            Assert.assertEquals("hithere", sqlgGraph1.traversal().V(a).outE("knows").properties("name").next().value());
        }

        //from topology
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(2, sqlgGraph1.traversal().V().count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().E().count().next().intValue());
            Assert.assertTrue(sqlgGraph1.traversal().V().hasLabel("A").hasNext());
            Assert.assertTrue(sqlgGraph1.traversal().V().hasLabel("B").hasNext());
            Assert.assertEquals(1, sqlgGraph1.traversal().V().hasLabel("A").count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().V().hasLabel("B").count().next().intValue());
            Vertex a = sqlgGraph1.traversal().V().hasLabel("A").next();
            Assert.assertEquals(idA1, a.id());
            Vertex b = sqlgGraph1.traversal().V().hasLabel("B").next();
            Assert.assertEquals(idB1, b.id());
            Assert.assertEquals(1, sqlgGraph1.traversal().V(a).out("knows").count().next().intValue());
            Assert.assertEquals(b, sqlgGraph1.traversal().V(a).out("knows").next());
            Assert.assertEquals(1, sqlgGraph1.traversal().V(b).in("knows").count().next().intValue());
            Assert.assertEquals(a, sqlgGraph1.traversal().V(b).in("knows").next());
            Assert.assertEquals(1, sqlgGraph1.traversal().V(a).properties("name").count().next().intValue());
            Assert.assertTrue(sqlgGraph1.traversal().V(a).properties("name").next().isPresent());
            Assert.assertEquals("john", sqlgGraph1.traversal().V(a).properties("name").next().value());
            Assert.assertEquals(1, sqlgGraph1.traversal().V(a).outE("knows").properties("name").count().next().intValue());
            Assert.assertTrue(sqlgGraph1.traversal().V(a).outE("knows").properties("name").next().isPresent());
            Assert.assertEquals("hithere", sqlgGraph1.traversal().V(a).outE("knows").properties("name").next().value());
        }
    }

    @Test
    public void testUpgradeMultipleInOutEdges() throws Exception {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Object a1Id = a1.id();
        Object b1Id = b1.id();
        a1.addEdge("ab", b1, "weight", 5);
        a1.addEdge("ab", c1, "weight", 6);
        b1.addEdge("ba", a1, "wtf", "wtf1");
        b1.addEdge("ba", c1, "wtf", "wtf1");
        this.sqlgGraph.tx().commit();
        //Delete the topology
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute("DROP SCHEMA " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("sqlg_schema")
                    + (this.sqlgGraph.getSqlDialect().needsSchemaDropCascade() ? " CASCADE" : ""));
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        //topology will be recreated
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(2, sqlgGraph1.traversal().V(a1Id).out().count().next().intValue());
            Assert.assertEquals(2, sqlgGraph1.traversal().V(b1Id).out().count().next().intValue());
        }
    }

    @Test
    public void testUpgradeMultipleInOutEdges2() throws Exception {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "R_EG.B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "P_EG.C", "name", "c1");
        Object a1Id = a1.id();
        Object b1Id = b1.id();
        a1.addEdge("ab", b1, "weight", 5);
        a1.addEdge("ab", c1, "weight", 6);
        b1.addEdge("ba", a1, "wtf", "wtf1");
        b1.addEdge("ba", c1, "wtf", "wtf1");
        this.sqlgGraph.tx().commit();
        //Delete the topology
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute("DROP SCHEMA " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("sqlg_schema") +
                    (this.sqlgGraph.getSqlDialect().needsSchemaDropCascade() ? " CASCADE" : ""));
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        //topology will be recreated
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(2, sqlgGraph1.traversal().V(a1Id).out().count().next().intValue());
            Assert.assertEquals(2, sqlgGraph1.traversal().V(b1Id).out().count().next().intValue());
        }
    }

    @Test
    public void multipleSchemas() throws Exception {

        Vertex aPublic = this.sqlgGraph.addVertex(T.label, "APUBLIC", "name", "aPublic");
        Vertex aReal = this.sqlgGraph.addVertex(T.label, "REAL.AREAL", "name", "aReal");
        aPublic.addEdge("a", aReal, "name", "asd");
        aReal.addEdge("a", aPublic, "name", "dsa");
        Vertex bPublic = this.sqlgGraph.addVertex(T.label, "BPUBLIC", "name", "bPublic");
        Vertex bReal = this.sqlgGraph.addVertex(T.label, "REAL.BREAL", "name", "bReal");
        bPublic.addEdge("a", bReal, "name", "asd");
        bReal.addEdge("a", bPublic, "name", "dsa");
        this.sqlgGraph.tx().commit();

        //Delete the topology
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute("DROP SCHEMA " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("sqlg_schema")
                    + (this.sqlgGraph.getSqlDialect().needsSchemaDropCascade() ? " CASCADE" : ""));
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        //topology will be recreated
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(1, sqlgGraph1.traversal().V(aPublic.id()).in().count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().V(aPublic.id()).out().count().next().intValue());
        }
    }

    @Test
    public void testGratefulDeadDBUpgrade() throws Exception {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
        Graph g = this.sqlgGraph;
        GraphReader reader = GryoReader.build()
                .mapper(g.io(GryoIo.build()).mapper().create())
                .create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/grateful-dead.kryo")) {
            reader.readGraph(stream, g);
        }
        Traversal<Vertex, Long> traversal = get_g_V_both_both_count(g.traversal());
        Assert.assertEquals(new Long(1406914), traversal.next());
        Assert.assertFalse(traversal.hasNext());

        //Delete the topology
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute("DROP SCHEMA " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("sqlg_schema") + " CASCADE");
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            sqlgGraph1.tx().normalBatchModeOn();
            sqlgGraph1.traversal().V().forEachRemaining(Element::remove);
            sqlgGraph1.tx().commit();
            g = sqlgGraph1;
            reader = GryoReader.build()
                    .mapper(g.io(GryoIo.build()).mapper().create())
                    .create();
            try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/grateful-dead.kryo")) {
                reader.readGraph(stream, g);
            }
            traversal = get_g_V_both_both_count(g.traversal());
            Assert.assertEquals(new Long(1406914), traversal.next());
            Assert.assertFalse(traversal.hasNext());
        }
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            sqlgGraph1.tx().normalBatchModeOn();
            sqlgGraph1.traversal().V().forEachRemaining(Element::remove);
            sqlgGraph1.tx().commit();
            g = sqlgGraph1;
            reader = GryoReader.build()
                    .mapper(g.io(GryoIo.build()).mapper().create())
                    .create();
            try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/grateful-dead.kryo")) {
                reader.readGraph(stream, g);
            }
            traversal = get_g_V_both_both_count(g.traversal());
            Assert.assertEquals(new Long(1406914), traversal.next());
            Assert.assertFalse(traversal.hasNext());

        }
    }

    @Test
    public void testModernGraph() throws Exception {
        loadModern();
        Traversal<Vertex, Long> traversal = this.sqlgGraph.traversal().V().both().both().count();
        Assert.assertEquals(new Long(30), traversal.next());
        Assert.assertFalse(traversal.hasNext());
        this.sqlgGraph.traversal().V().forEachRemaining(Element::remove);
        this.sqlgGraph.tx().commit();
        //Delete the topology
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute("DROP SCHEMA " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("sqlg_schema") +
                    (this.sqlgGraph.getSqlDialect().needsSchemaDropCascade() ? " CASCADE" : ""));
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            loadModern(sqlgGraph1);
            traversal = sqlgGraph1.traversal().V().both().both().count();
            Assert.assertEquals(new Long(30), traversal.next());
            Assert.assertFalse(traversal.hasNext());
        }
    }

    @Test
    public void testTopologyFilter() {
        Vertex god = this.sqlgGraph.addVertex(T.label, "God", "name", "god1");
        Vertex universe = this.sqlgGraph.addVertex(T.label, "Universe", "name", "universe1");
        god.addEdge("universe", universe);
        this.sqlgGraph.tx().commit();
        GraphTraversalSource traversalSource = this.sqlgGraph.traversal().withStrategies(
                TopologyStrategy.build().selectFrom(
                        SchemaManager.SQLG_SCHEMA_SCHEMA_TABLES
                ).create()
        );
        List<Vertex> schemas = traversalSource.V()
                .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA)
                .toList();
        Assert.assertEquals(1, schemas.size());
        Long count = this.sqlgGraph.topology().V().count().next();
        Assert.assertEquals(6, count, 0);
    }

    @Test
    public void testUpgradePropertiesAcrossSchema() throws Exception {
        this.sqlgGraph.addVertex(T.label, "A.A", "name", "a1", "name1", "a11");
        this.sqlgGraph.addVertex(T.label, "B.A", "name", "b1", "name2", "b22");
        this.sqlgGraph.tx().commit();
        //Delete the topology
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute("DROP SCHEMA " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("sqlg_schema")
                    + (this.sqlgGraph.getSqlDialect().needsSchemaDropCascade() ? " CASCADE" : ""));
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<Vertex> schemaVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A").toList();
            Assert.assertEquals(1, schemaVertices.size());
            List<Vertex> propertyVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A")
                    .out("schema_vertex")
                    .has("name", "A")
                    .out("vertex_property").toList();
            Assert.assertEquals(2, propertyVertices.size());
            propertyVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "B")
                    .out("schema_vertex").has("name", "A")
                    .out("vertex_property").toList();
            Assert.assertEquals(2, propertyVertices.size());
        }
    }


    private Traversal<Vertex, Long> get_g_V_both_both_count(GraphTraversalSource g) {
        return g.V().both().both().count();
    }
}
