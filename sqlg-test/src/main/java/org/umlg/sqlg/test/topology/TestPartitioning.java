package org.umlg.sqlg.test.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.time.LocalDate;
import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/01/13
 */
public class TestPartitioning extends BaseTest {

    @Before
    public void before() throws Exception {
        super.before();
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsPartitioning());
    }

    @Test
    public void testReloadVertexLabelWithNoPartitions() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<String, PropertyType>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("int1")),
                PartitionType.RANGE,
                "int1,int2");
        this.sqlgGraph.tx().commit();

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            VertexLabel a = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").get();
            Assert.assertEquals("int1,int2", a.getPartitionExpression());
            Assert.assertEquals(PartitionType.RANGE, a.getPartitionType());
        }
    }

    @Test
    public void testReloadVertexLabelWithPartitions() throws Exception {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<String, PropertyType>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("int1")),
                PartitionType.RANGE,
                "int1,int2");
        a.ensureRangePartitionExists("part1", "1,1", "5,5");
        a.ensureRangePartitionExists("part2", "5,5", "10,10");
        this.sqlgGraph.tx().commit();

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            a = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").get();
            Assert.assertEquals("int1,int2", a.getPartitionExpression());
            Assert.assertEquals(PartitionType.RANGE, a.getPartitionType());

            Optional<Partition> part1 = a.getPartition("part1");
            Assert.assertTrue(part1.isPresent());
            Assert.assertNull(part1.get().getPartitionExpression());
            Assert.assertEquals("1, 1", part1.get().getFrom());
            Assert.assertEquals("5, 5", part1.get().getTo());
            Optional<Partition> part2 = a.getPartition("part2");
            Assert.assertTrue(part2.isPresent());
            Assert.assertNull(part2.get().getPartitionExpression());
            Assert.assertEquals("5, 5", part2.get().getFrom());
            Assert.assertEquals("10, 10", part2.get().getTo());
        }
    }

    @Test
    public void testReloadVertexLabelWithSubPartitions() throws Exception {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<String, PropertyType>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("int1", "int2")),
                PartitionType.RANGE,
                "int1,int2");
        Partition part1 = a.ensureRangePartitionWithSubPartitionExists("part1", "1,1", "5,5", PartitionType.RANGE, "int3");
        Partition part2 = a.ensureRangePartitionWithSubPartitionExists("part2", "5,5", "10,10", PartitionType.RANGE, "int3");

        part1.ensureRangePartitionExists("part11", "1", "5");
        part2.ensureRangePartitionExists("part21", "1", "5");

        this.sqlgGraph.tx().commit();

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            a = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").get();
            Assert.assertEquals("int1,int2", a.getPartitionExpression());
            Assert.assertEquals(PartitionType.RANGE, a.getPartitionType());

            Optional<Partition> p1 = a.getPartition("part1");
            Assert.assertTrue(p1.isPresent());
            Assert.assertEquals("int3", p1.get().getPartitionExpression());
            Assert.assertEquals(PartitionType.RANGE, p1.get().getPartitionType());
            Assert.assertEquals("1, 1", p1.get().getFrom());
            Assert.assertEquals("5, 5", p1.get().getTo());

            Optional<Partition> p11 = p1.get().getPartition("part11");
            Assert.assertTrue(p11.isPresent());
            Assert.assertNull(p11.get().getPartitionExpression());
            Assert.assertEquals(PartitionType.NONE, p11.get().getPartitionType());
            Assert.assertEquals("1", p11.get().getFrom());
            Assert.assertEquals("5", p11.get().getTo());

            Optional<Partition> p2 = a.getPartition("part2");
            Assert.assertTrue(p2.isPresent());
            Assert.assertEquals("int3", p2.get().getPartitionExpression());
            Assert.assertEquals(PartitionType.RANGE, p2.get().getPartitionType());
            Assert.assertEquals("5, 5", p2.get().getFrom());
            Assert.assertEquals("10, 10", p2.get().getTo());

            Optional<Partition> p21 = p2.get().getPartition("part21");
            Assert.assertTrue(p21.isPresent());
            Assert.assertNull(p21.get().getPartitionExpression());
            Assert.assertEquals(PartitionType.NONE, p21.get().getPartitionType());
            Assert.assertEquals("1", p21.get().getFrom());
            Assert.assertEquals("5", p21.get().getTo());
        }
    }

    @Test
    public void testReloadEdgeLabelWithNoPartitions() throws Exception {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensureVertexLabelExist(
                "A",
                Collections.emptyMap()
        );
        VertexLabel b = publicSchema.ensureVertexLabelExist(
                "B",
                Collections.emptyMap()
        );
        a.ensurePartitionedEdgeLabelExist(
                "ab",
                b,
                new LinkedHashMap<String, PropertyType>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("int1", "int2")),
                PartitionType.RANGE,
                "int1,int2"
        );
        this.sqlgGraph.tx().commit();

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            EdgeLabel edgeLabel = sqlgGraph1.getTopology()
                    .getPublicSchema()
                    .getEdgeLabel("ab").get();
            Assert.assertEquals("int1,int2", edgeLabel.getPartitionExpression());
            Assert.assertEquals(PartitionType.RANGE, edgeLabel.getPartitionType());
        }
    }

    @Test
    public void testReloadEdgeLabelWithPartitions() throws Exception {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensureVertexLabelExist(
                "A",
                Collections.emptyMap()
        );
        VertexLabel b = publicSchema.ensureVertexLabelExist(
                "B",
                Collections.emptyMap()
        );
        EdgeLabel ab = a.ensurePartitionedEdgeLabelExist(
                "ab",
                b,
                new LinkedHashMap<String, PropertyType>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("int1", "int2")),
                PartitionType.LIST,
                "int1"
        );
        Partition p1 = ab.ensureListPartitionExists("int1", "1,2,3,4,5");
        Partition p2 = ab.ensureListPartitionExists("int2", "6,7,8,9,10");

        this.sqlgGraph.tx().commit();

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            EdgeLabel edgeLabel = sqlgGraph1.getTopology()
                    .getPublicSchema()
                    .getEdgeLabel("ab").get();
            Assert.assertEquals("int1", edgeLabel.getPartitionExpression());
            Assert.assertEquals(PartitionType.LIST, edgeLabel.getPartitionType());

            Assert.assertEquals(2, edgeLabel.getPartitions().size());
            Assert.assertTrue(edgeLabel.getPartitions().containsKey("int1"));
            Assert.assertTrue(edgeLabel.getPartitions().containsKey("int2"));

            p1 = edgeLabel.getPartition("int1").get();
            Assert.assertEquals(PartitionType.NONE, p1.getPartitionType());
            Assert.assertEquals("1, 2, 3, 4, 5", p1.getIn());
            p2 = edgeLabel.getPartition("int2").get();
            Assert.assertEquals(PartitionType.NONE, p2.getPartitionType());
            Assert.assertEquals("6, 7, 8, 9, 10", p2.getIn());
        }
    }

    @Test
    public void testReloadEdgeLabelWithSubPartitions() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensureVertexLabelExist(
                "A",
                Collections.emptyMap()
        );
        VertexLabel b = publicSchema.ensureVertexLabelExist(
                "B",
                Collections.emptyMap()
        );
        EdgeLabel ab = a.ensurePartitionedEdgeLabelExist(
                "ab",
                b,
                new LinkedHashMap<String, PropertyType>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("int1", "int2")),
                PartitionType.LIST,
                "int1"
        );
        Partition p1 = ab.ensureListPartitionWithSubPartitionExists("p11", "1,2,3,4,5", PartitionType.RANGE, "int2");
        Partition p111 = p1.ensureRangePartitionWithSubPartitionExists("p111", "1", "5", PartitionType.LIST, "int3");
        p111.ensureListPartitionExists("p1111", "1,2,3,4,5");
        Partition p2 = ab.ensureListPartitionWithSubPartitionExists("p12", "6,7,8,9,10", PartitionType.RANGE, "int2");
        Partition p121 = p2.ensureRangePartitionWithSubPartitionExists("p121", "1", "5", PartitionType.LIST, "int3");
        p121.ensureListPartitionExists("p1211", "1,2,3,4,10");

        this.sqlgGraph.tx().commit();

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            EdgeLabel edgeLabel = sqlgGraph1.getTopology()
                    .getPublicSchema()
                    .getEdgeLabel("ab").get();
            Assert.assertEquals("int1", edgeLabel.getPartitionExpression());
            Assert.assertEquals(PartitionType.LIST, edgeLabel.getPartitionType());

            Assert.assertEquals(2, edgeLabel.getPartitions().size());
            Assert.assertTrue(edgeLabel.getPartitions().containsKey("p11"));
            Assert.assertTrue(edgeLabel.getPartitions().containsKey("p12"));

            p1 = edgeLabel.getPartition("p11").get();
            Assert.assertEquals(PartitionType.RANGE, p1.getPartitionType());
            Assert.assertEquals("1, 2, 3, 4, 5", p1.getIn());
            Assert.assertEquals(1, p1.getPartitions().size());
            Assert.assertTrue(p1.getPartitions().containsKey("p111"));
            p111 = p1.getPartitions().get("p111");
            Assert.assertEquals(PartitionType.LIST, p111.getPartitionType());
            Assert.assertEquals("int3", p111.getPartitionExpression());
            Assert.assertEquals("1", p111.getFrom());
            Assert.assertEquals("5", p111.getTo());
            Assert.assertTrue(p111.getPartition("p1111").isPresent());
            Partition p1111 = p111.getPartition("p1111").get();
            Assert.assertTrue(p1111.getPartitionType().isNone());
            Assert.assertNull(p1111.getPartitionExpression());
            Assert.assertEquals("1, 2, 3, 4, 5", p1111.getIn());


            p2 = edgeLabel.getPartition("p12").get();
            Assert.assertEquals(PartitionType.RANGE, p2.getPartitionType());
            Assert.assertEquals("6, 7, 8, 9, 10", p2.getIn());
            Assert.assertEquals(1, p2.getPartitions().size());
            Assert.assertTrue(p2.getPartitions().containsKey("p121"));
            p121 = p2.getPartitions().get("p121");
            Assert.assertEquals(PartitionType.LIST, p121.getPartitionType());
            Assert.assertEquals("int3", p121.getPartitionExpression());
            Assert.assertEquals("1", p121.getFrom());
            Assert.assertEquals("5", p121.getTo());
            Assert.assertTrue(p121.getPartition("p1211").isPresent());
            Partition p1211 = p121.getPartition("p1211").get();
            Assert.assertTrue(p1211.getPartitionType().isNone());
            Assert.assertNull(p1211.getPartitionExpression());
            Assert.assertEquals("1, 2, 3, 4, 10", p1211.getIn());
        }
    }

    @Test
    public void testPartitioningRange() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel partitionedVertexLabel = publicSchema.ensurePartitionedVertexLabelExist(
                "Measurement",
                new LinkedHashMap<String, PropertyType>() {{
                    put("date", PropertyType.LOCALDATE);
                    put("temp", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("date")),
                PartitionType.RANGE,
                "date");
        partitionedVertexLabel.ensureRangePartitionExists("measurement1", "'2016-07-01'", "'2016-08-01'");
        partitionedVertexLabel.ensureRangePartitionExists("measurement2", "'2016-08-01'", "'2016-09-01'");
        this.sqlgGraph.tx().commit();

        LocalDate localDate1 = LocalDate.of(2016, 7, 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "date", localDate1);
        LocalDate localDate2 = LocalDate.of(2016, 8, 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "date", localDate2);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("Measurement").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Measurement").has("date", localDate1).count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Measurement").has("date", localDate2).count().next(), 0);

        Partition partition = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Measurement").get().getPartition("measurement1").get();
        partition.remove();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Measurement").count().next(), 0);
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("Measurement").has("date", localDate1).count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Measurement").has("date", localDate2).count().next(), 0);

        Assert.assertEquals(1, this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PARTITION).count().next(), 0);
    }


    //the partitionExpression 'left(lower(name), 1)' is to complex for the query planner to optimize.
    //i.e. select * from Cities where name = 'asdasd' willscan all partitions.
    @Test
    public void testPartitioningList() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel partitionedVertexLabel = publicSchema.ensurePartitionedVertexLabelExist("Cities",
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                    put("population", PropertyType.LONG);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name")),
                PartitionType.LIST,
                "left(lower(name), 1)");
        partitionedVertexLabel.ensureListPartitionExists("Cities_a", "'a'");
        partitionedVertexLabel.ensureListPartitionExists("Cities_b", "'b'");
        partitionedVertexLabel.ensureListPartitionExists("Cities_c", "'c'");
        partitionedVertexLabel.ensureListPartitionExists("Cities_d", "'d'");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "Cities", "name", "aasbc", "population", 1000L);
        }
        this.sqlgGraph.addVertex(T.label, "Cities", "name", "basbc", "population", 1000L);
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "Cities", "name", "casbc", "population", 1000L);
        }
        this.sqlgGraph.addVertex(T.label, "Cities", "name", "dasbc", "population", 1000L);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(202, this.sqlgGraph.traversal().V().hasLabel("Cities").count().next(), 0);
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("Cities").has("name", "aasbc").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Cities").has("name", "basbc").count().next(), 0);
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("Cities").has("name", "casbc").count().next(), 0);

        Partition partition = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Cities").get().getPartition("Cities_a").get();
        partition.remove();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(102, this.sqlgGraph.traversal().V().hasLabel("Cities").count().next(), 0);
        Assert.assertEquals(3, this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PARTITION).count().next(), 0);
    }

    //partitionExpression is simple enough for the query planner to optimize the queries.
    @Test
    public void testPartitioningListSimple() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel partitionedVertexLabel = publicSchema.ensurePartitionedVertexLabelExist("Cities",
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                    put("population", PropertyType.LONG);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name")),
                PartitionType.LIST,
                "name");
        partitionedVertexLabel.ensureListPartitionExists("Cities_a", "'London'");
        partitionedVertexLabel.ensureListPartitionExists("Cities_b", "'New York'");
        partitionedVertexLabel.ensureListPartitionExists("Cities_c", "'Paris'");
        partitionedVertexLabel.ensureListPartitionExists("Cities_d", "'Johannesburg'");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "Cities", "name", "London", "population", 1000L);
        }
        this.sqlgGraph.addVertex(T.label, "Cities", "name", "New York", "population", 1000L);
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "Cities", "name", "Paris", "population", 1000L);
        }
        this.sqlgGraph.addVertex(T.label, "Cities", "name", "Johannesburg", "population", 1000L);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(202, this.sqlgGraph.traversal().V().hasLabel("Cities").count().next(), 0);
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("Cities").has("name", "London").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Cities").has("name", "New York").count().next(), 0);
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("Cities").has("name", "Paris").count().next(), 0);

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(202, sqlgGraph1.traversal().V().hasLabel("Cities").count().next(), 0);
            Assert.assertEquals(100, sqlgGraph1.traversal().V().hasLabel("Cities").has("name", "London").count().next(), 0);
            Assert.assertEquals(1, sqlgGraph1.traversal().V().hasLabel("Cities").has("name", "New York").count().next(), 0);
            Assert.assertEquals(100, sqlgGraph1.traversal().V().hasLabel("Cities").has("name", "Paris").count().next(), 0);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testPartitionRangeInSchema() {
        Schema testSchema = this.sqlgGraph.getTopology().ensureSchemaExist("test");
        VertexLabel partitionedVertexLabel = testSchema.ensurePartitionedVertexLabelExist("Measurement",
                new LinkedHashMap<String, PropertyType>() {{
                    put("date", PropertyType.LOCALDATE);
                    put("temp", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("date")),
                PartitionType.RANGE,
                "date");
        partitionedVertexLabel.ensureRangePartitionExists("measurement1", "'2016-07-01'", "'2016-08-01'");
        partitionedVertexLabel.ensureRangePartitionExists("measurement2", "'2016-08-01'", "'2016-09-01'");
        this.sqlgGraph.tx().commit();

        LocalDate localDate1 = LocalDate.of(2016, 7, 1);
        this.sqlgGraph.addVertex(T.label, "test.Measurement", "date", localDate1);
        LocalDate localDate2 = LocalDate.of(2016, 8, 1);
        this.sqlgGraph.addVertex(T.label, "test.Measurement", "date", localDate2);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("test.Measurement").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("test.Measurement").has("date", localDate1).count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("test.Measurement").has("date", localDate2).count().next(), 0);

        Partition partition = this.sqlgGraph.getTopology().getSchema("test").get().getVertexLabel("Measurement").get().getPartition("measurement1").get();
        partition.remove();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("test.Measurement").count().next(), 0);
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("test.Measurement").has("date", localDate1).count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("test.Measurement").has("date", localDate2).count().next(), 0);

        Assert.assertEquals(1, this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PARTITION).count().next(), 0);

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {

            Assert.assertEquals(1, sqlgGraph1.traversal().V().hasLabel("test.Measurement").count().next(), 0);
            Assert.assertEquals(0, sqlgGraph1.traversal().V().hasLabel("test.Measurement").has("date", localDate1).count().next(), 0);
            Assert.assertEquals(1, sqlgGraph1.traversal().V().hasLabel("test.Measurement").has("date", localDate2).count().next(), 0);

            Assert.assertEquals(1, sqlgGraph1.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PARTITION).count().next(), 0);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testPartitionedEdgesRange() {
        VertexLabel person = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person");
        VertexLabel address = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Address");
        EdgeLabel livedAt = person.ensurePartitionedEdgeLabelExist(
                "liveAt",
                address,
                new LinkedHashMap<String, PropertyType>() {{
                    put("date", PropertyType.LOCALDATE);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("date")),
                PartitionType.RANGE,
                "date");
        Partition p1 = livedAt.ensureRangePartitionExists("livedAt1", "'2016-07-01'", "'2016-08-01'");
        Partition p2 = livedAt.ensureRangePartitionExists("livedAt2", "'2016-08-01'", "'2016-09-01'");
        this.sqlgGraph.tx().commit();

        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "Address");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "Address");
        person1.addEdge("liveAt", a1, "date", LocalDate.of(2016, 7, 1));
        person1.addEdge("liveAt", a2, "date", LocalDate.of(2016, 8, 2));
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraph.traversal().E().hasLabel("liveAt").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("liveAt").has("date", LocalDate.of(2016, 7, 1)).count().next(), 0);

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(2, this.sqlgGraph.traversal().E().hasLabel("liveAt").count().next(), 0);
            Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("liveAt").has("date", LocalDate.of(2016, 7, 1)).count().next(), 0);
            Assert.assertEquals(2, sqlgGraph1.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PARTITION).count().next(), 0);

            Assert.assertEquals(
                    "date",
                    sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("liveAt").get().getPartitionExpression()
            );
            Assert.assertEquals(
                    PartitionType.RANGE,
                    sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("liveAt").get().getPartitionType()
            );
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionedEdgesList() {
        VertexLabel person = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person");
        VertexLabel address = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Address");
        EdgeLabel livedAt = person.ensurePartitionedEdgeLabelExist(
                "liveAt",
                address,
                new LinkedHashMap<String, PropertyType>() {{
                    put("date", PropertyType.LOCALDATE);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("date")),
                PartitionType.LIST,
                "date");
        Partition p1 = livedAt.ensureListPartitionExists("livedAt1", "'2016-07-01'");
        Partition p2 = livedAt.ensureListPartitionExists("livedAt2", "'2016-07-02'");
        Partition p3 = livedAt.ensureListPartitionExists("livedAt2", "'2016-07-03'");
        Partition p4 = livedAt.ensureListPartitionExists("livedAt2", "'2016-07-04'");
        this.sqlgGraph.tx().commit();

        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "Address");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "Address");
        person1.addEdge("liveAt", a1, "date", LocalDate.of(2016, 7, 1));
        person1.addEdge("liveAt", a2, "date", LocalDate.of(2016, 7, 2));
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraph.traversal().E().hasLabel("liveAt").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("liveAt").has("date", LocalDate.of(2016, 7, 1)).count().next(), 0);

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(2, this.sqlgGraph.traversal().E().hasLabel("liveAt").count().next(), 0);
            Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("liveAt").has("date", LocalDate.of(2016, 7, 1)).count().next(), 0);
            Assert.assertEquals(2, sqlgGraph1.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PARTITION).count().next(), 0);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSubPartitioningRange() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel measurement = publicSchema.ensurePartitionedVertexLabelExist("Measurement",
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                    put("logdate", PropertyType.LOCALDATE);
                    put("peaktemp", PropertyType.INTEGER);
                    put("unitsales", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name")),
                PartitionType.RANGE,
                "logdate");

        Partition p1 = measurement.ensureRangePartitionWithSubPartitionExists("measurement_y2006m02", "'2006-02-01'", "'2006-03-01'", PartitionType.RANGE, "peaktemp");
        Partition p2 = measurement.ensureRangePartitionWithSubPartitionExists("measurement_y2006m03", "'2006-03-01'", "'2006-04-01'", PartitionType.RANGE, "peaktemp");
        p1.ensureRangePartitionExists("peaktemp1", "1", "2");
        p1.ensureRangePartitionExists("peaktemp2", "2", "3");
        p2.ensureRangePartitionExists("peaktemp3", "1", "2");
        p2.ensureRangePartitionExists("peaktemp4", "2", "3");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "logdate", LocalDate.of(2006, 2, 1),
                "peaktemp", 1, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement2", "logdate", LocalDate.of(2006, 2, 2),
                "peaktemp", 1, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement3", "logdate", LocalDate.of(2006, 3, 1),
                "peaktemp", 1, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement4", "logdate", LocalDate.of(2006, 3, 2),
                "peaktemp", 2, "unitsales", 1);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertexList = this.sqlgGraph.traversal().V()
                .hasLabel("Measurement")
                .has("logdate", LocalDate.of(2006, 2, 1))
                .has("peaktemp", 1)
                .toList();
        Assert.assertEquals(1, vertexList.size());
    }

    @Test
    public void testSubPartitioningList() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel measurement = publicSchema.ensurePartitionedVertexLabelExist("Measurement",
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                    put("list1", PropertyType.STRING);
                    put("list2", PropertyType.INTEGER);
                    put("unitsales", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name")),
                PartitionType.LIST,
                "list1");

        Partition p1 = measurement.ensureListPartitionWithSubPartitionExists("measurement_list1", "'1'", PartitionType.LIST, "list2");
        Partition p2 = measurement.ensureListPartitionWithSubPartitionExists("measurement_list2", "'2'", PartitionType.LIST, "list2");
        p1.ensureListPartitionExists("measurement_list1_1", "1");
        p1.ensureListPartitionExists("measurement_list1_2", "2");
        p1.ensureListPartitionExists("measurement_list1_3", "3");
        p1.ensureListPartitionExists("measurement_list1_4", "4");

        p2.ensureListPartitionExists("measurement_list2_1", "1");
        p2.ensureListPartitionExists("measurement_list2_2", "2");
        p2.ensureListPartitionExists("measurement_list2_3", "3");
        p2.ensureListPartitionExists("measurement_list2_4", "4");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "list1", "1", "list2", 1, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement2", "list1", "1", "list2", 2, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement3", "list1", "1", "list2", 3, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement4", "list1", "1", "list2", 4, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement5", "list1", "1", "list2", 4, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement6", "list1", "1", "list2", 4, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement7", "list1", "1", "list2", 4, "unitsales", 1);

        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement8", "list1", "2", "list2", 1, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement9", "list1", "2", "list2", 2, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement10", "list1", "2", "list2", 3, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement11", "list1", "2", "list2", 4, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement12", "list1", "2", "list2", 4, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement13", "list1", "2", "list2", 4, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement14", "list1", "2", "list2", 4, "unitsales", 1);

        this.sqlgGraph.tx().commit();

        List<Vertex> vertexList = this.sqlgGraph.traversal().V()
                .hasLabel("Measurement")
                .has("list1", "1")
                .toList();
        Assert.assertEquals(7, vertexList.size());

        vertexList = this.sqlgGraph.traversal().V()
                .hasLabel("Measurement")
                .has("list1", "2")
                .has("list2", 4)
                .toList();
        Assert.assertEquals(4, vertexList.size());
    }

    @Test
    public void testEdgeSubPartitioningRange() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensureVertexLabelExist("A");
        VertexLabel b = publicSchema.ensureVertexLabelExist("B");
        EdgeLabel ab = a.ensurePartitionedEdgeLabelExist(
                "ab",
                b,
                new LinkedHashMap<String, PropertyType>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("int1")),
                PartitionType.RANGE,
                "int1");
        Partition p11 = ab.ensureRangePartitionWithSubPartitionExists("int1_1_4", "1", "4", PartitionType.RANGE, "int2");
        Partition p12 = ab.ensureRangePartitionWithSubPartitionExists("int1_4_8", "4", "8", PartitionType.RANGE, "int2");
        Partition p13 = ab.ensureRangePartitionWithSubPartitionExists("int1_8_12", "8", "12", PartitionType.RANGE, "int2");

        p11.ensureRangePartitionExists("int2_1_4", "1", "4");
        p11.ensureRangePartitionExists("int2_4_8", "4", "8");
        p11.ensureRangePartitionExists("int2_8_12", "8", "12");
        p11.ensureRangePartitionExists("int2_12_16", "12", "16");

        p12.ensureRangePartitionExists("int22_1_4", "1", "4");
        p12.ensureRangePartitionExists("int22_4_8", "4", "8");
        p12.ensureRangePartitionExists("int22_8_12", "8", "12");
        p12.ensureRangePartitionExists("int22_12_16", "12", "16");

        p13.ensureRangePartitionExists("int23_1_4", "1", "4");
        p13.ensureRangePartitionExists("int23_4_8", "4", "8");
        p13.ensureRangePartitionExists("int23_8_12", "8", "12");
        p13.ensureRangePartitionExists("int23_12_16", "12", "16");
        this.sqlgGraph.tx().commit();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Vertex a1 = sqlgGraph1.addVertex(T.label, "A");
            Vertex b1 = sqlgGraph1.addVertex(T.label, "B");
            a1.addEdge("ab", b1, "int1", 2, "int2", 2);
            a1.addEdge("ab", b1, "int1", 6, "int2", 6);
            a1.addEdge("ab", b1, "int1", 10, "int2", 10);
            a1.addEdge("ab", b1, "int1", 11, "int2", 14);
            sqlgGraph1.tx().commit();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        Assert.assertEquals(4, this.sqlgGraph.traversal().V().hasLabel("A").out().count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").outE().has("int1", 6).has("int2", 6).otherV().count().next(), 0);

    }

    @Test
    public void testEdgeSubPartitioningList() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensureVertexLabelExist("A");
        VertexLabel b = publicSchema.ensureVertexLabelExist("B");
        EdgeLabel ab = a.ensurePartitionedEdgeLabelExist(
                "ab",
                b,
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid", PropertyType.STRING);
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid")),
                PartitionType.LIST,
                "int1");
        Partition p11 = ab.ensureListPartitionWithSubPartitionExists("int1_1_5", "1,2,3,4,5", PartitionType.LIST, "int2");
        Partition p12 = ab.ensureListPartitionWithSubPartitionExists("int1_5_10", "6,7,8,9,10", PartitionType.LIST, "int2");

        p11.ensureListPartitionExists("int2_11_15", "11,12,13,14,15");
        p11.ensureListPartitionExists("int2_16_20", "16,17,18,19,20");

        p12.ensureListPartitionExists("int22_11_15", "11,12,13,14,15");
        p12.ensureListPartitionExists("int22_16_20", "16,17,18,19,20");

        this.sqlgGraph.tx().commit();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Vertex a1 = sqlgGraph1.addVertex(T.label, "A");
            Vertex b1 = sqlgGraph1.addVertex(T.label, "B");
            a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString(), "int1", 2, "int2", 12);
            a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString(), "int1", 6, "int2", 13);
            a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString(), "int1", 10, "int2", 14);
            a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString(), "int1", 10, "int2", 15);
            sqlgGraph1.tx().commit();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        Assert.assertEquals(4, this.sqlgGraph.traversal().V().hasLabel("A").out().count().next(), 0);
        Assert.assertEquals(
                1,
                this.sqlgGraph.traversal()
                        .V().hasLabel("A")
                        .outE().has("int1", 6).has("int2", 13)
                        .otherV().count().next(), 0);

    }
}
