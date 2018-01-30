package org.umlg.sqlg.test.topology;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/01/13
 */
public class TestPartitioning extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws PropertyVetoException, IOException, ClassNotFoundException {
        BaseTest.beforeClass();
        Assume.assumeTrue(isPostgres());
    }

    @Test
    public void testPartitioningRange() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel partitionedVertexLabel = publicSchema.ensurePartitionedVertexLabelExist("Measurement", new HashMap<String, PropertyType>() {{
                    put("date", PropertyType.LOCALDATE);
                    put("temp", PropertyType.INTEGER);
                }},
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
        VertexLabel partitionedVertexLabel = publicSchema.ensurePartitionedVertexLabelExist("Cities", new HashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                    put("population", PropertyType.LONG);
                }},
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
        VertexLabel partitionedVertexLabel = publicSchema.ensurePartitionedVertexLabelExist("Cities", new HashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                    put("population", PropertyType.LONG);
                }},
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
        VertexLabel partitionedVertexLabel = testSchema.ensurePartitionedVertexLabelExist("Measurement", new HashMap<String, PropertyType>() {{
                    put("date", PropertyType.LOCALDATE);
                    put("temp", PropertyType.INTEGER);
                }},
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
        EdgeLabel livedAt = person.ensurePartitionedEdgeLabelExist("liveAt", address, new HashMap<String, PropertyType>() {{
            put("date", PropertyType.LOCALDATE);
        }}, PartitionType.RANGE, "date");
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
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionedEdgesList() {
        VertexLabel person = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person");
        VertexLabel address = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Address");
        EdgeLabel livedAt = person.ensurePartitionedEdgeLabelExist("liveAt", address, new HashMap<String, PropertyType>() {{
            put("date", PropertyType.LOCALDATE);
        }}, PartitionType.LIST, "date");
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
        VertexLabel measurement = publicSchema.ensurePartitionedVertexLabelExist("Measurement", new HashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                    put("logdate", PropertyType.LOCALDATE);
                    put("peaktemp", PropertyType.INTEGER);
                    put("unitsales", PropertyType.INTEGER);
                }},
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
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "logdate", LocalDate.of(2006, 2, 2),
                "peaktemp", 1, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "logdate", LocalDate.of(2006, 3, 1),
                "peaktemp", 1, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "logdate", LocalDate.of(2006, 3, 2),
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
        VertexLabel measurement = publicSchema.ensurePartitionedVertexLabelExist("Measurement", new HashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                    put("list1", PropertyType.STRING);
                    put("list2", PropertyType.INTEGER);
                    put("unitsales", PropertyType.INTEGER);
                }},
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
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "list1", "1", "list2", 2, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "list1", "1", "list2", 3, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "list1", "1", "list2", 4, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "list1", "1", "list2", 4, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "list1", "1", "list2", 4, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "list1", "1", "list2", 4, "unitsales", 1);

        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "list1", "2", "list2", 1, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "list1", "2", "list2", 2, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "list1", "2", "list2", 3, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "list1", "2", "list2", 4, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "list1", "2", "list2", 4, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "list1", "2", "list2", 4, "unitsales", 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "name", "measurement1", "list1", "2", "list2", 4, "unitsales", 1);

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
                new HashMap<String, PropertyType>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                }},
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
            a1.addEdge("ab", b1, "int1", 10, "int2", 14);
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
                new HashMap<String, PropertyType>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                }},
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
            a1.addEdge("ab", b1, "int1", 2, "int2", 12);
            a1.addEdge("ab", b1, "int1", 6, "int2", 13);
            a1.addEdge("ab", b1, "int1", 10, "int2", 14);
            a1.addEdge("ab", b1, "int1", 10, "int2", 15);
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
