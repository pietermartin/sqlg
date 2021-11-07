package org.umlg.sqlg.test.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.strategy.TopologyStrategy;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.Statement;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Date: 2016/02/06
 * Time: 6:17 PM
 */
public class TestTopologyUpgrade extends BaseTest {

    @Test
    public void testTopologyUpgradeForeignKey() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            System.out.println("asd");
        }
    }

    //The asdasd index is for a property that foes not exist, it will be ignored when loading the topology.
    @Test
    public void testCustomIndexIgnored() throws Exception {
        //TODO one day for HSQLDB and the rest
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().isPostgresql());

        Map<String, PropertyType> properties = new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
        }};
        Schema hour = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel vertexLabel = hour.ensureVertexLabelExist("A", properties);
        PropertyColumn propertyColumn = vertexLabel.getProperty("name").get();
        vertexLabel.ensureIndexExists(IndexType.NON_UNIQUE, Collections.singletonList(propertyColumn));

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A.A", "name", "aaaa");
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute("CREATE INDEX \"asdasd\" ON \"A\".\"V_A\" USING btree (left('name', 1)) ");
        }
        this.sqlgGraph.tx().commit();
        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(1, sqlgGraph1.traversal().V().count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.getTopology().getSchema("A").get().getVertexLabel("A").get().getIndexes().size());
        }
    }

    @Test
    public void testUpgradeEdgeWithMultipleInOutLabelsAndAIndex() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B");
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("C");
        Map<String, PropertyType> properties = new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
        }};
        @SuppressWarnings("UnusedAssignment")
        EdgeLabel edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureEdgeLabelExist("edge", bVertexLabel, aVertexLabel, properties);
        edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureEdgeLabelExist("edge", cVertexLabel, aVertexLabel, properties);
        edgeLabel.ensureIndexExists(IndexType.UNIQUE, new ArrayList<>(edgeLabel.getProperties().values()));
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        b1.addEdge("edge", a1, "name", "b");
        c1.addEdge("edge", a1, "name", "c");
        this.sqlgGraph.tx().commit();

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(3, sqlgGraph1.traversal().V().count().next().intValue());
            Assert.assertEquals(2, sqlgGraph1.traversal().E().count().next().intValue());
        }
    }

    @Test
    public void testUpgrade() {
        //with topology
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "john");
        Object idA1 = a1.id();
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "joe");
        Object idB1 = b1.id();
        a1.addEdge("knows", b1, "name", "hithere");
        this.sqlgGraph.tx().commit();

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);
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
    public void testUpgradeMultipleInOutEdges() {
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
        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        //topology will be recreated
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(2, sqlgGraph1.traversal().V(a1Id).out().count().next().intValue());
            Assert.assertEquals(2, sqlgGraph1.traversal().V(b1Id).out().count().next().intValue());
        }
    }

    @Test
    public void testUpgradeMultipleInOutEdges2() {
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
        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        //topology will be recreated
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(2, sqlgGraph1.traversal().V(a1Id).out().count().next().intValue());
            Assert.assertEquals(2, sqlgGraph1.traversal().V(b1Id).out().count().next().intValue());
        }
    }

    @Test
    public void multipleSchemas() {

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
        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        //topology will be recreated
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(1, sqlgGraph1.traversal().V(aPublic.id()).in().count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().V(aPublic.id()).out().count().next().intValue());
        }
    }

    @Test
    public void testGratefulDeadDBUpgrade() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
        loadGratefulDead(this.sqlgGraph);
        Traversal<Vertex, Long> traversal = get_g_V_both_both_count(this.sqlgGraph.traversal());
        Assert.assertEquals(new Long(1406914), traversal.next());
        Assert.assertFalse(traversal.hasNext());

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.close();

        for (int i = 0; i < 2; i++) {
            try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
                traversal = get_g_V_both_both_count(sqlgGraph1.traversal());
                Assert.assertEquals(new Long(1406914), traversal.next());
                Assert.assertFalse(traversal.hasNext());
            }
        }
    }

    @Test
    public void testModernGraph() {
        loadModern();
        Traversal<Vertex, Long> traversal = this.sqlgGraph.traversal().V().both().both().count();
        Assert.assertEquals(new Long(30), traversal.next());
        Assert.assertFalse(traversal.hasNext());
        this.sqlgGraph.traversal().V().forEachRemaining(Element::remove);
        this.sqlgGraph.tx().commit();
        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);
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
                TopologyStrategy.build().sqlgSchema().create()
        );
        List<Vertex> schemas = traversalSource.V()
                .hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .toList();
        //public
        Assert.assertEquals(1, schemas.size());
        Long count = this.sqlgGraph.topology().V().count().next();
        Assert.assertEquals(7, count, 0);
    }

    @Test
    public void testUpgradePropertiesAcrossSchema() {
        this.sqlgGraph.addVertex(T.label, "A.A", "name", "a1", "name1", "a11");
        this.sqlgGraph.addVertex(T.label, "B.A", "name", "b1", "name2", "b22");
        this.sqlgGraph.tx().commit();
        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);
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

    @Test
    public void testUpgradeTypesWithMoreThanOneColumn() {
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        if (isHsqldb() ||isMariaDb()) {
            zonedDateTime = zonedDateTime.truncatedTo(ChronoUnit.MILLIS);
        }
        Duration duration = Duration.ofDays(1);
        Period period = Period.of(1, 1, 1);
        this.sqlgGraph.addVertex(T.label, "A.A", "name", "a1", "zonedDateTime", zonedDateTime);
        this.sqlgGraph.addVertex(T.label, "A.A", "name", "a2", "duration", duration);
        this.sqlgGraph.addVertex(T.label, "A.A", "name", "a3", "period", period);
        this.sqlgGraph.tx().commit();
        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<Vertex> schemaVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A").toList();
            Assert.assertEquals(1, schemaVertices.size());
            //assert zonedDateTime
            List<Vertex> propertyVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A")
                    .out("schema_vertex")
                    .has("name", "A")
                    .out("vertex_property")
                    .has("name", "zonedDateTime").toList();
            Assert.assertEquals(1, propertyVertices.size());
            Assert.assertEquals(PropertyType.ZONEDDATETIME, PropertyType.valueOf(propertyVertices.get(0).value("type")));
            List<Vertex> vertices = sqlgGraph1.traversal().V().hasLabel("A.A").has("name", "a1").toList();
            Assert.assertEquals(1, vertices.size());
            Assert.assertEquals(zonedDateTime, vertices.get(0).value("zonedDateTime"));

            //assert duration
            propertyVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A")
                    .out("schema_vertex")
                    .has("name", "A")
                    .out("vertex_property")
                    .has("name", "duration").toList();
            Assert.assertEquals(1, propertyVertices.size());
            Assert.assertEquals(PropertyType.DURATION, PropertyType.valueOf(propertyVertices.get(0).value("type")));
            vertices = sqlgGraph1.traversal().V().hasLabel("A.A").has("name", "a2").toList();
            Assert.assertEquals(1, vertices.size());
            Assert.assertEquals(duration, vertices.get(0).value("duration"));

            //assert period
            propertyVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A")
                    .out("schema_vertex")
                    .has("name", "A")
                    .out("vertex_property")
                    .has("name", "period").toList();
            Assert.assertEquals(1, propertyVertices.size());
            Assert.assertEquals(PropertyType.PERIOD, PropertyType.valueOf(propertyVertices.get(0).value("type")));
            vertices = sqlgGraph1.traversal().V().hasLabel("A.A").has("name", "a3").toList();
            Assert.assertEquals(1, vertices.size());
            Assert.assertEquals(period, vertices.get(0).value("period"));
        }
    }

    @Test
    public void testUpgradeTypesWithMoreThanOneColumnOnEdge() {
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            zonedDateTime = zonedDateTime.truncatedTo(ChronoUnit.MILLIS);
        }
        Duration duration = Duration.ofDays(1);
        Period period = Period.of(1, 1, 1);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "a2");
        a1.addEdge("ab", a2, "zonedDateTime", zonedDateTime, "duration", duration, "period", period);
        this.sqlgGraph.tx().commit();

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<Vertex> schemaVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A").toList();
            Assert.assertEquals(1, schemaVertices.size());
            //assert zonedDateTime
            List<Vertex> propertyVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A")
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "A")
                    .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .out(Topology.SQLG_SCHEMA_EDGE_PROPERTIES_EDGE)
                    .has(Topology.SQLG_SCHEMA_PROPERTY_NAME, "zonedDateTime").toList();
            Assert.assertEquals(1, propertyVertices.size());
            Assert.assertEquals(PropertyType.ZONEDDATETIME, PropertyType.valueOf(propertyVertices.get(0).value("type")));
            List<Edge> edges = sqlgGraph1.traversal().E().hasLabel("ab").toList();
            Assert.assertEquals(1, edges.size());
            Assert.assertEquals(zonedDateTime, edges.get(0).value("zonedDateTime"));
            Assert.assertEquals(duration, edges.get(0).value("duration"));
            Assert.assertEquals(period, edges.get(0).value("period"));
        }
    }

    @Test
    public void testUpgradeArrayTypesWithMoreThanOneColumn() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsZonedDateTimeArrayValues());
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDurationArrayValues());
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsPeriodArrayValues());
        ZonedDateTime[] zonedDateTimes = new ZonedDateTime[]{
                isHsqldb() ? ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS) : ZonedDateTime.now(),
                isHsqldb() ? ZonedDateTime.now().minusMonths(1).truncatedTo(ChronoUnit.MILLIS) : ZonedDateTime.now().minusMonths(1),
                isHsqldb() ? ZonedDateTime.now().minusMonths(2).truncatedTo(ChronoUnit.MILLIS) : ZonedDateTime.now().minusMonths(2)
        };
        Duration[] durations = new Duration[]{Duration.ofDays(1), Duration.ofDays(2), Duration.ofDays(3)};
        Period[] periods = new Period[]{Period.of(1, 1, 1), Period.of(2, 2, 2), Period.of(3, 3, 3)};
        this.sqlgGraph.addVertex(T.label, "A.A", "name", "a1", "zonedDateTimes", zonedDateTimes);
        this.sqlgGraph.addVertex(T.label, "A.A", "name", "a2", "durations", durations);
        this.sqlgGraph.addVertex(T.label, "A.A", "name", "a3", "periods", periods);
        this.sqlgGraph.tx().commit();
        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<Vertex> schemaVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A").toList();
            Assert.assertEquals(1, schemaVertices.size());
            //assert zonedDateTimes
            List<Vertex> propertyVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A")
                    .out("schema_vertex")
                    .has("name", "A")
                    .out("vertex_property")
                    .has("name", "zonedDateTimes").toList();
            Assert.assertEquals(1, propertyVertices.size());
            Assert.assertEquals(PropertyType.ZONEDDATETIME_ARRAY, PropertyType.valueOf(propertyVertices.get(0).value("type")));
            List<Vertex> vertices = sqlgGraph1.traversal().V().hasLabel("A.A").has("name", "a1").toList();
            Assert.assertEquals(1, vertices.size());
            Assert.assertArrayEquals(zonedDateTimes, vertices.get(0).value("zonedDateTimes"));

            //assert durations
            propertyVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A")
                    .out("schema_vertex")
                    .has("name", "A")
                    .out("vertex_property")
                    .has("name", "durations").toList();
            Assert.assertEquals(1, propertyVertices.size());
            Assert.assertEquals(PropertyType.DURATION_ARRAY, PropertyType.valueOf(propertyVertices.get(0).value("type")));
            vertices = sqlgGraph1.traversal().V().hasLabel("A.A").has("name", "a2").toList();
            Assert.assertEquals(1, vertices.size());
            Assert.assertArrayEquals(durations, vertices.get(0).value("durations"));

            //assert periods
            propertyVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A")
                    .out("schema_vertex")
                    .has("name", "A")
                    .out("vertex_property")
                    .has("name", "periods").toList();
            Assert.assertEquals(1, propertyVertices.size());
            Assert.assertEquals(PropertyType.PERIOD_ARRAY, PropertyType.valueOf(propertyVertices.get(0).value("type")));
            vertices = sqlgGraph1.traversal().V().hasLabel("A.A").has("name", "a3").toList();
            Assert.assertEquals(1, vertices.size());
            Assert.assertArrayEquals(periods, vertices.get(0).value("periods"));
        }
    }

    @Test
    public void testUpgradeTypesWithMoreThanOneColumnOnEdgeArrays() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDurationArrayValues());
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsPeriodArrayValues());
        ZonedDateTime[] zonedDateTimes = new ZonedDateTime[]{
                isHsqldb() ? ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS) : ZonedDateTime.now(),
                isHsqldb() ? ZonedDateTime.now().minusMonths(1).truncatedTo(ChronoUnit.MILLIS) : ZonedDateTime.now().minusMonths(1),
                isHsqldb() ? ZonedDateTime.now().minusMonths(2).truncatedTo(ChronoUnit.MILLIS) : ZonedDateTime.now().minusMonths(2)
        };
        Duration[] durations = new Duration[]{Duration.ofDays(1), Duration.ofDays(2), Duration.ofDays(3)};
        Period[] periods = new Period[]{Period.of(1, 1, 1), Period.of(2, 2, 2), Period.of(3, 3, 3)};

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "a2");
        a1.addEdge("ab", a2, "zonedDateTimes", zonedDateTimes, "durations", durations, "periods", periods);
        this.sqlgGraph.tx().commit();

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<Vertex> schemaVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A").toList();
            Assert.assertEquals(1, schemaVertices.size());
            //assert zonedDateTime
            List<Vertex> propertyVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A")
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "A")
                    .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .out(Topology.SQLG_SCHEMA_EDGE_PROPERTIES_EDGE)
                    .has(Topology.SQLG_SCHEMA_PROPERTY_NAME, "zonedDateTimes").toList();
            Assert.assertEquals(1, propertyVertices.size());
            Assert.assertEquals(PropertyType.ZONEDDATETIME_ARRAY, PropertyType.valueOf(propertyVertices.get(0).value("type")));
            List<Edge> edges = sqlgGraph1.traversal().E().hasLabel("ab").toList();
            Assert.assertEquals(1, edges.size());
            Assert.assertArrayEquals(zonedDateTimes, edges.get(0).value("zonedDateTimes"));
            Assert.assertArrayEquals(durations, edges.get(0).value("durations"));
            Assert.assertArrayEquals(periods, edges.get(0).value("periods"));
        }
    }

    @Test
    public void testUpgradeJson() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsJsonType());
        ObjectNode json = new ObjectMapper().createObjectNode();
        json.put("halo", "asdasd");
        this.sqlgGraph.addVertex(T.label, "A.A", "name", "a1", "json", json);
        this.sqlgGraph.tx().commit();
        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<Vertex> schemaVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A").toList();
            Assert.assertEquals(1, schemaVertices.size());
            //assert zonedDateTimes
            List<Vertex> propertyVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A")
                    .out("schema_vertex")
                    .has("name", "A")
                    .out("vertex_property")
                    .has("name", "json").toList();
            Assert.assertEquals(1, propertyVertices.size());
            Assert.assertEquals(PropertyType.JSON, PropertyType.valueOf(propertyVertices.get(0).value("type")));
            List<Vertex> vertices = sqlgGraph1.traversal().V().hasLabel("A.A").has("name", "a1").toList();
            Assert.assertEquals(1, vertices.size());
            Assert.assertEquals(json, vertices.get(0).value("json"));
        }
    }

    @Test
    public void testUpgradeJsonArrays() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().isPostgresql());
        //HSQLDB can not upgrade JSON ARRAY as its indistinguishable from STRING ARRAY.
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsJsonArrayValues());
        ObjectNode json1 = new ObjectMapper().createObjectNode();
        json1.put("halo", "asdasd");
        ObjectNode json2 = new ObjectMapper().createObjectNode();
        json2.put("halo", "asdasd");
        ObjectNode[] jsons = new ObjectNode[]{json1, json2};
        this.sqlgGraph.addVertex(T.label, "A.A", "name", "a1", "jsons", jsons);
        this.sqlgGraph.tx().commit();
        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<Vertex> schemaVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A").toList();
            Assert.assertEquals(1, schemaVertices.size());
            //assert zonedDateTimes
            List<Vertex> propertyVertices = sqlgGraph1.topology().V().hasLabel("sqlg_schema.schema").has("name", "A")
                    .out("schema_vertex")
                    .has("name", "A")
                    .out("vertex_property")
                    .has("name", "jsons").toList();
            Assert.assertEquals(1, propertyVertices.size());
            Assert.assertEquals(PropertyType.JSON_ARRAY, PropertyType.valueOf(propertyVertices.get(0).value("type")));
            List<Vertex> vertices = sqlgGraph1.traversal().V().hasLabel("A.A").has("name", "a1").toList();
            Assert.assertEquals(1, vertices.size());
            Assert.assertArrayEquals(jsons, vertices.get(0).value("jsons"));
        }
    }

    @Test
    public void testUpdateLocalDateTimeAndZonedDateTime() {
        LocalDateTime localDateTime = LocalDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            localDateTime = localDateTime.truncatedTo(ChronoUnit.MILLIS);
        }
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            zonedDateTime = zonedDateTime.truncatedTo(ChronoUnit.MILLIS);
        }
        this.sqlgGraph.addVertex(T.label, "A", "localDateTime", localDateTime, "zonedDateTime", zonedDateTime);
        this.sqlgGraph.tx().commit();

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(localDateTime, sqlgGraph1.traversal().V().hasLabel("A").next().value("localDateTime"));
            Assert.assertEquals(zonedDateTime, sqlgGraph1.traversal().V().hasLabel("A").next().value("zonedDateTime"));
        }
    }

    @Test
    public void testUpgradeArrays() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsShortArrayValues());
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDoubleArrayValues());
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalDateArrayValues());
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalDateTimeArrayValues());
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalTimeArrayValues());
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsZonedDateTimeArrayValues());

        Byte[] bytes = new Byte[]{1, 2, 3};
        byte[] bytes2 = new byte[]{1, 2, 3};
        Short[] shorts = new Short[]{1, 2, 3};
        short[] shorts2 = new short[]{1, 2, 3};
        Integer[] integers = new Integer[]{1, 2, 3};
        int[] integers2 = new int[]{1, 2, 3};
        Long[] longs = new Long[]{1L, 2L, 3L};
        long[] longs2 = new long[]{1L, 2L, 3L};
        Double[] doubles = new Double[]{1D, 2D, 3D};
        double[] doubles2 = new double[]{1D, 2D, 3D};
        String[] strings = new String[]{"a", "b", "c"};
        LocalDate[] localDates = new LocalDate[]{LocalDate.now(), LocalDate.now().minusMonths(2), LocalDate.now().minusMonths(3)};
        LocalDateTime[] localDateTimes = new LocalDateTime[]{
                isHsqldb() ? LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS) : LocalDateTime.now(),
                isHsqldb() ? LocalDateTime.now().minusMonths(2).truncatedTo(ChronoUnit.MILLIS) : LocalDateTime.now().minusMonths(2),
                isHsqldb() ? LocalDateTime.now().minusMonths(3).truncatedTo(ChronoUnit.MILLIS) : LocalDateTime.now().minusMonths(3)
        };
        LocalTime[] localTimes = new LocalTime[]{LocalTime.now(), LocalTime.now().minusHours(2), LocalTime.now().minusHours(3)};
        ZonedDateTime[] zonedDateTimes = new ZonedDateTime[]{
                isHsqldb() ? ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS) : ZonedDateTime.now(),
                isHsqldb() ? ZonedDateTime.now().minusHours(2).truncatedTo(ChronoUnit.MILLIS) : ZonedDateTime.now().minusHours(2),
                isHsqldb() ? ZonedDateTime.now().minusHours(3).truncatedTo(ChronoUnit.MILLIS) : ZonedDateTime.now().minusHours(3)
        };
        this.sqlgGraph.addVertex(T.label, "A",
                "bytes", bytes, "bytes2", bytes2,
                "shorts", shorts, "shorts2", shorts2,
                "integers", integers, "integers2", integers2,
                "longs", longs, "longs2", longs2,
                "doubles", doubles, "doubles2", doubles2,
                "strings", strings,
                "localDates", localDates,
                "localDateTimes", localDateTimes,
                "localTimes", localTimes,
                "zonedDateTimes", zonedDateTimes
        );
        this.sqlgGraph.tx().commit();

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertArrayEquals(bytes, sqlgGraph1.traversal().V().hasLabel("A").next().value("bytes"));
            Assert.assertArrayEquals(bytes, sqlgGraph1.traversal().V().hasLabel("A").next().value("bytes2"));
            Assert.assertArrayEquals(shorts, sqlgGraph1.traversal().V().hasLabel("A").next().value("shorts"));
            Assert.assertArrayEquals(shorts, sqlgGraph1.traversal().V().hasLabel("A").next().value("shorts2"));
            Assert.assertArrayEquals(integers, sqlgGraph1.traversal().V().hasLabel("A").next().value("integers"));
            Assert.assertArrayEquals(integers, sqlgGraph1.traversal().V().hasLabel("A").next().value("integers2"));
            Assert.assertArrayEquals(longs, sqlgGraph1.traversal().V().hasLabel("A").next().value("longs"));
            Assert.assertArrayEquals(longs, sqlgGraph1.traversal().V().hasLabel("A").next().value("longs2"));
            Assert.assertArrayEquals(doubles, sqlgGraph1.traversal().V().hasLabel("A").next().value("doubles"));
            Assert.assertArrayEquals(doubles, sqlgGraph1.traversal().V().hasLabel("A").next().value("doubles2"));
            Assert.assertArrayEquals(strings, sqlgGraph1.traversal().V().hasLabel("A").next().value("strings"));
            Assert.assertArrayEquals(localDates, sqlgGraph1.traversal().V().hasLabel("A").next().value("localDates"));
            Assert.assertArrayEquals(localDateTimes, sqlgGraph1.traversal().V().hasLabel("A").next().value("localDateTimes"));
            LocalTime[] value = sqlgGraph1.traversal().V().hasLabel("A").next().value("localTimes");
            List<LocalTime> localTimes1 = new ArrayList<>();
            for (LocalTime localTime : value) {
                localTimes1.add(localTime.minusNanos(localTime.getNano()));
            }
            Assert.assertArrayEquals(localTimes1.toArray(), value);
            Assert.assertArrayEquals(zonedDateTimes, sqlgGraph1.traversal().V().hasLabel("A").next().value("zonedDateTimes"));
        }
    }

    @Test
    public void testUpgradeFloatArrays() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        Float[] floats = new Float[]{1F, 2F, 3F};
        float[] floats2 = new float[]{1F, 2F, 3F};
        this.sqlgGraph.addVertex(T.label, "A",
                "floats", floats, "floats2", floats2
        );
        this.sqlgGraph.tx().commit();

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertArrayEquals(floats, sqlgGraph1.traversal().V().hasLabel("A").next().value("floats"));
            Assert.assertArrayEquals(floats, sqlgGraph1.traversal().V().hasLabel("A").next().value("floats2"));
        }
    }

    private Traversal<Vertex, Long> get_g_V_both_both_count(GraphTraversalSource g) {
        return g.V().both().both().count();
    }

    @SuppressWarnings("serial")
    @Test
    public void testUpgradeIndex() {
        //with topology
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new HashMap<String, PropertyType>() {{
                    put("firstName", PropertyType.STRING);
                    put("lastName", PropertyType.STRING);
                }}
        );
        personVertexLabel.ensureIndexExists(IndexType.UNIQUE, new ArrayList<>(personVertexLabel.getProperties().values()));
        Map<String, PropertyType> properties = new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
        }};
        VertexLabel dogVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("Dog", properties);
        dogVertexLabel.ensureIndexExists(IndexType.NON_UNIQUE, new ArrayList<>(dogVertexLabel.getProperties().values()));

        Map<String, PropertyType> eproperties = new HashMap<String, PropertyType>() {{
            put("since", PropertyType.LOCALDATE);
        }};
        EdgeLabel eLabel = this.sqlgGraph.getTopology().ensureEdgeLabelExist("Owns",
                personVertexLabel,
                dogVertexLabel,
                eproperties);
        eLabel.ensureIndexExists(IndexType.UNIQUE, new ArrayList<>(eLabel.getProperties().values()));

        // test performance
        /*for (int a=0;a<1000;a++){
        	VertexLabel testVertex = this.sqlgGraph.getTopology().ensureVertexLabelExist("Person"+a, new HashMap<String, PropertyType>() {{
                put("firstName", PropertyType.STRING);
                put("lastName", PropertyType.STRING);
            }});
        	testVertex.ensureIndexExists(IndexType.UNIQUE, new ArrayList<>(testVertex.getProperties().values()));

        }*/

        // not supported yet
        /*this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<PropertyColumn>() {{
            add(personVertexLabel.getProperty("firstName").get());
            add(dogVertexLabel.getProperty("name").get());
        }});*/
        this.sqlgGraph.tx().commit();

        // sanity
        topologyCheck(this.sqlgGraph);

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        //topology will be recreated
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            topologyCheck(sqlgGraph1);
        }

        //from topology
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            topologyCheck(sqlgGraph1);
        }
    }

    @Test
    public void testNonEdgeNonVerticeTablesIgnored() throws Exception {
        //Drop these tables in case they are hanging around, Sqlg only drops V_ and E_ tables.
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()) + "." +
                    this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("EVENT"));
        }
        try (Statement statement = conn.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()) + "." +
                    this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("VIKINGS"));
        }
        sqlgGraph.tx().commit();

        //with topology
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "john");
        Object idA1 = a1.id();
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "joe");
        Object idB1 = b1.id();
        a1.addEdge("knows", b1, "name", "hithere");
        this.sqlgGraph.tx().commit();

        // Create some user tables that should be ignored by sqlg
        conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute(this.sqlgGraph.getSqlDialect().createTableStatement() + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()) + "." +
                    this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("EVENT") + "(ID INT)");
        }
        try (Statement statement = conn.createStatement()) {
            statement.execute(this.sqlgGraph.getSqlDialect().createTableStatement() + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()) + "." +
                    this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("VIKINGS") + "(ID INT)");
        }
        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);

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
            Assert.assertEquals(2, sqlgGraph1.getTopology().getPublicSchema().getVertexLabels().size());

            conn = sqlgGraph1.tx().getConnection();
            try (Statement statement = conn.createStatement()) {
                statement.execute("DROP TABLE " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()) + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("EVENT"));
            }
            try (Statement statement = conn.createStatement()) {
                statement.execute("DROP TABLE " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()) + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("VIKINGS"));
            }
            sqlgGraph1.tx().commit();
        }
    }

    private static void topologyCheck(SqlgGraph sqlgGraph) {
        Assert.assertEquals(1, sqlgGraph.topology().V().has(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL, Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "Person").count().next().longValue());

        Assert.assertEquals(1, sqlgGraph.topology().V().has(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL, Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "Dog").count().next().longValue());

        Assert.assertEquals(1, sqlgGraph.topology().V()
                .has(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL, Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "Person")
                .out(Topology.SQLG_SCHEMA_VERTEX_INDEX_EDGE)
                .count().next().longValue());

        Assert.assertEquals(2, sqlgGraph.topology().V()
                .has(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL, Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "Person")
                .out(Topology.SQLG_SCHEMA_VERTEX_INDEX_EDGE)
                .out(Topology.SQLG_SCHEMA_INDEX_PROPERTY_EDGE)
                .count().next().longValue());

        Assert.assertEquals(1, sqlgGraph.topology().V()
                .has(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL, Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "Person")
                .out(Topology.SQLG_SCHEMA_VERTEX_INDEX_EDGE)
                .out(Topology.SQLG_SCHEMA_INDEX_PROPERTY_EDGE)
                .has(Topology.SQLG_SCHEMA_PROPERTY_NAME, "firstName")
                .count().next().longValue());

        Assert.assertEquals(1, sqlgGraph.topology().V()
                .has(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL, Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "Person")
                .out(Topology.SQLG_SCHEMA_VERTEX_INDEX_EDGE)
                .out(Topology.SQLG_SCHEMA_INDEX_PROPERTY_EDGE)
                .has(Topology.SQLG_SCHEMA_PROPERTY_NAME, "lastName")
                .count().next().longValue());

        Assert.assertEquals(1, sqlgGraph.topology().V()
                .has(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL, Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "Dog")
                .out(Topology.SQLG_SCHEMA_VERTEX_INDEX_EDGE)
                .count().next().longValue());

        Assert.assertEquals(1, sqlgGraph.topology().V()
                .has(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL, Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "Dog")
                .out(Topology.SQLG_SCHEMA_VERTEX_INDEX_EDGE)
                .out(Topology.SQLG_SCHEMA_INDEX_PROPERTY_EDGE)
                .count().next().longValue());

        Assert.assertEquals(1, sqlgGraph.topology().V()
                .has(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL, Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "Dog")
                .out(Topology.SQLG_SCHEMA_VERTEX_INDEX_EDGE)
                .out(Topology.SQLG_SCHEMA_INDEX_PROPERTY_EDGE)
                .has(Topology.SQLG_SCHEMA_PROPERTY_NAME, "name")
                .count().next().longValue());

        Assert.assertEquals(1, sqlgGraph.topology().V()
                .has(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_EDGE_LABEL, Topology.SQLG_SCHEMA_EDGE_LABEL_NAME, "Owns").count().next().longValue());

        Assert.assertEquals(1, sqlgGraph.topology().V()
                .has(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_EDGE_LABEL, Topology.SQLG_SCHEMA_EDGE_LABEL_NAME, "Owns")
                .out(Topology.SQLG_SCHEMA_EDGE_INDEX_EDGE)
                .count().next().longValue());

        Assert.assertEquals(1, sqlgGraph.topology().V()
                .has(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_EDGE_LABEL, Topology.SQLG_SCHEMA_EDGE_LABEL_NAME, "Owns")
                .out(Topology.SQLG_SCHEMA_EDGE_INDEX_EDGE)
                .out(Topology.SQLG_SCHEMA_INDEX_PROPERTY_EDGE)
                .has(Topology.SQLG_SCHEMA_PROPERTY_NAME, "since")
                .count().next().longValue());

        // not supported yet
        /*assertEquals(1,sqlgGraph.topology().V()
                .hasLabel(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX).count().next().longValue());

        Assert.assertEquals(2,sqlgGraph.topology().V()
        		.hasLabel(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX)
        		.out(Topology.SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE)
        		.count().next().longValue());

        Assert.assertEquals(2,sqlgGraph.topology().V()
        		.hasLabel(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX)
        		.out(Topology.SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE)
        		.count().next().longValue());

        Assert.assertEquals(1,sqlgGraph.topology().V()
        		.hasLabel(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX)
        		.out(Topology.SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE)
        		.has(Topology.SQLG_SCHEMA_PROPERTY_NAME,"firstName")
        		.count().next().longValue());

        Assert.assertEquals(1,sqlgGraph.topology().V()
        		.hasLabel(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX)
        		.out(Topology.SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE)
        		.has(Topology.SQLG_SCHEMA_PROPERTY_NAME,"name")
        		.count().next().longValue());*/
    }

}
