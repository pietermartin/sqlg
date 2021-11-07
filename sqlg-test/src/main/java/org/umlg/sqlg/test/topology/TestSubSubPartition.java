package org.umlg.sqlg.test.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/01/26
 */
public class TestSubSubPartition extends BaseTest {

    @SuppressWarnings("Duplicates")
    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            Assume.assumeTrue(isPostgres());
            configuration.addProperty("distributed", true);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void before() throws Exception {
        super.before();
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsPartitioning());
    }

    @Test
    public void testVertexSubSubPartitionRange() throws Exception {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensurePartitionedVertexLabelExist(
                "A",
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid", "int1", "int2", "int3")),
                PartitionType.RANGE,
                "int1");
        Partition p1 = a.ensureRangePartitionWithSubPartitionExists("int1", "1", "5", PartitionType.RANGE, "int2");
        Partition p2 = a.ensureRangePartitionWithSubPartitionExists("int2", "5", "10", PartitionType.RANGE, "int2");
        this.sqlgGraph.tx().commit();
        Thread.sleep(1000);

        Partition p1_1 = p1.ensureRangePartitionWithSubPartitionExists("int11", "1", "5", PartitionType.RANGE, "int3");
        Partition p1_2 = p1.ensureRangePartitionWithSubPartitionExists("int12", "5", "10", PartitionType.RANGE, "int3");
        Partition p2_1 = p2.ensureRangePartitionWithSubPartitionExists("int21", "1", "5", PartitionType.RANGE, "int3");
        this.sqlgGraph.tx().commit();
        Thread.sleep(1000);
        Partition p2_2 = p2.ensureRangePartitionWithSubPartitionExists("int22", "5", "10", PartitionType.RANGE, "int3");

        p1_1.ensureRangePartitionExists("int111", "1", "5");
        p1_1.ensureRangePartitionExists("int112", "5", "10");
        p1_2.ensureRangePartitionExists("int121", "1", "5");
        this.sqlgGraph.tx().commit();
        Thread.sleep(1000);
        p1_2.ensureRangePartitionExists("int122", "5", "10");
        p2_1.ensureRangePartitionExists("int211", "1", "5");
        p2_1.ensureRangePartitionExists("int212", "5", "10");
        p2_2.ensureRangePartitionExists("int221", "1", "5");
        p2_2.ensureRangePartitionExists("int222", "5", "10");
        this.sqlgGraph.tx().commit();
        Thread.sleep(2000);

        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "int1", 1, "int2", 1, "int3", 1);
        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "int1", 5, "int2", 5, "int3", 5);

        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("int1", 1).has("int2", 1).has("int3", 1).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("int1", 5).has("int2", 5).has("int3", 5).toList();
        Assert.assertEquals(1, vertices.size());

        assert_vertexLabelSubSubPartition(this.sqlgGraph);
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            assert_vertexLabelSubSubPartition(sqlgGraph1);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        assert_vertexLabelSubSubPartition(this.sqlgGraph1);

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            assert_vertexLabelSubSubPartition(sqlgGraph1);
            a = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").get();
            p2 = a.getPartition("int2").get();
            p2.remove();
            Assert.assertEquals(1, a.getPartitions().size());
            Assert.assertEquals(7, sqlgGraph1.topology().V().hasLabel("sqlg_schema.partition").count().next(), 0);
        }
    }

    private void assert_vertexLabelSubSubPartition(SqlgGraph sqlgGraph1) {
        List<Vertex> vertices;VertexLabel vertexLabel = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").get();
        Map<String, Partition> partitions = vertexLabel.getPartitions();
        Assert.assertEquals(2, partitions.size());
        Assert.assertTrue(partitions.containsKey("int1"));
        Partition pint1 = partitions.get("int1");
        Assert.assertTrue(pint1.getPartitionType().isRange());
        Assert.assertEquals("int2", pint1.getPartitionExpression());

        Assert.assertEquals(2, pint1.getPartitions().size());
        Assert.assertTrue(pint1.getPartitions().containsKey("int11"));
        Partition pint11 = pint1.getPartitions().get("int11");
        Assert.assertTrue(pint11.getPartitionType().isRange());
        Assert.assertEquals("int3", pint11.getPartitionExpression());

        Assert.assertEquals(2, pint11.getPartitions().size());
        Assert.assertTrue(pint11.getPartitions().containsKey("int111"));
        Partition pint111 = pint11.getPartitions().get("int111");
        Assert.assertTrue(pint111.getPartitionType().isNone());
        Assert.assertNull(pint111.getPartitionExpression());

        Assert.assertEquals(0, pint111.getPartitions().size());
        Assert.assertTrue(pint11.getPartitions().containsKey("int112"));
        Partition pint112 = pint11.getPartitions().get("int112");
        Assert.assertEquals(0, pint112.getPartitions().size());

        Assert.assertTrue(pint1.getPartitions().containsKey("int12"));
        Partition pint12 = pint1.getPartitions().get("int12");
        Assert.assertEquals(2, pint12.getPartitions().size());
        Assert.assertTrue(pint12.getPartitions().containsKey("int121"));
        Partition pint121 = pint12.getPartitions().get("int121");
        Assert.assertEquals(0, pint121.getPartitions().size());
        Assert.assertTrue(pint12.getPartitions().containsKey("int122"));
        Partition pint122 = pint12.getPartitions().get("int122");
        Assert.assertEquals(0, pint122.getPartitions().size());

        Assert.assertTrue(partitions.containsKey("int2"));
        Partition pint2 = partitions.get("int2");
        Assert.assertTrue(pint2.getPartitionType().isRange());
        Assert.assertEquals("int2", pint2.getPartitionExpression());
        Assert.assertEquals(2, pint2.getPartitions().size());
        Assert.assertTrue(pint2.getPartitions().containsKey("int21"));
        Partition pint21 = pint2.getPartitions().get("int21");
        Assert.assertTrue(pint21.getPartitionType().isRange());
        Assert.assertEquals("int3", pint21.getPartitionExpression());
        Assert.assertEquals(2, pint21.getPartitions().size());
        Assert.assertTrue(pint21.getPartitions().containsKey("int211"));
        Partition pint211 = pint21.getPartitions().get("int211");
        Assert.assertEquals(0, pint211.getPartitions().size());
        Assert.assertTrue(pint21.getPartitions().containsKey("int212"));
        Partition pint212 = pint21.getPartitions().get("int212");
        Assert.assertTrue(pint212.getPartitionType().isNone());
        Assert.assertNull(pint212.getPartitionExpression());
        Assert.assertEquals(0, pint212.getPartitions().size());

        Assert.assertTrue(pint2.getPartitions().containsKey("int22"));
        Partition pint22 = pint2.getPartitions().get("int22");
        Assert.assertEquals(2, pint22.getPartitions().size());
        Assert.assertTrue(pint22.getPartitions().containsKey("int221"));
        Partition pint221 = pint22.getPartitions().get("int221");
        Assert.assertEquals(0, pint221.getPartitions().size());
        Assert.assertTrue(pint22.getPartitions().containsKey("int222"));
        Partition pint222 = pint22.getPartitions().get("int222");
        Assert.assertEquals(0, pint222.getPartitions().size());

        Assert.assertEquals(14, sqlgGraph1.topology().V().hasLabel("sqlg_schema.partition").count().next(), 0);

        vertices = sqlgGraph1.traversal().V().hasLabel("A").has("int1", 1).has("int2", 1).has("int3", 1).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = sqlgGraph1.traversal().V().hasLabel("A").has("int1", 5).has("int2", 5).has("int3", 5).toList();
        Assert.assertEquals(1, vertices.size());
        sqlgGraph1.tx().rollback();
    }

    @Test
    public void testEdgeSubSubPartitionRange() throws Exception {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensureVertexLabelExist("A");
        VertexLabel b = publicSchema.ensureVertexLabelExist("B");
        EdgeLabel ab = a.ensurePartitionedEdgeLabelExist(
                "ab",
                b,
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid", "int1", "int2", "int3")),
                PartitionType.RANGE,
                "int1");
        this.sqlgGraph.tx().commit();
        Thread.sleep(2000);

        Partition p1 = ab.ensureRangePartitionWithSubPartitionExists("int1", "1", "5", PartitionType.RANGE, "int2");
        Partition p2 = ab.ensureRangePartitionWithSubPartitionExists("int2", "5", "10", PartitionType.RANGE, "int2");

        Partition p11 = p1.ensureRangePartitionWithSubPartitionExists("int11", "1", "5", PartitionType.RANGE, "int3");
        Partition p12 = p1.ensureRangePartitionWithSubPartitionExists("int12", "5", "10", PartitionType.RANGE, "int3");
        Partition p21 = p2.ensureRangePartitionWithSubPartitionExists("int21", "1", "5", PartitionType.RANGE, "int3");
        Partition p22 = p2.ensureRangePartitionWithSubPartitionExists("int22", "5", "10", PartitionType.RANGE, "int3");

        p11.ensureRangePartitionExists("int111", "1", "5");
        p11.ensureRangePartitionExists("int112", "5", "10");
        p12.ensureRangePartitionExists("int121", "1", "5");
        p12.ensureRangePartitionExists("int122", "5", "10");
        p21.ensureRangePartitionExists("int221", "1", "5");
        p22.ensureRangePartitionExists("int222", "5", "10");

        this.sqlgGraph.tx().commit();
        Thread.sleep(2000);

        for (int i = 1; i < 10; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
            a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString(), "int1", i, "int2", i, "int3", i);
        }
        this.sqlgGraph.tx().commit();
        assert_edgePartitions(this.sqlgGraph);
        assert_edgePartitions(this.sqlgGraph1);

        Assert.assertEquals(12, this.sqlgGraph.topology().V().hasLabel("sqlg_schema.partition").count().next(), 0);
        EdgeLabel edgeLabel1 = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get();
        Optional<Partition> optionalPartition = edgeLabel1.getPartition("int1");
        Assert.assertTrue(optionalPartition.isPresent());
        Partition p = optionalPartition.get();
        optionalPartition = p.getPartition("int11");
        Assert.assertTrue(optionalPartition.isPresent());
        optionalPartition = edgeLabel1.getPartition("int2");
        Assert.assertTrue(optionalPartition.isPresent());
        p = optionalPartition.get();
        optionalPartition = p.getPartition("int22");
        Assert.assertTrue(optionalPartition.isPresent());

        List<Vertex> vertices = this.sqlgGraph.traversal().V().toList();
        Assert.assertEquals(18, vertices.size());
        List<Edge> edges = this.sqlgGraph.traversal().E().toList();
        Assert.assertEquals(9, edges.size());
        vertices = this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .outE("ab")
                .has("int1", 1)
                .has("int2", 1)
                .has("int3", 1)
                .otherV()
                .toList();
        Assert.assertEquals(1, vertices.size());

        assert_edgePartitions(this.sqlgGraph1);

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            assert_edgePartitions(sqlgGraph1);
        }

        p22.remove();
        this.sqlgGraph.tx().commit();
        Thread.sleep(2000);

        assert_edgePartitionsAfterRemove(this.sqlgGraph);
        assert_edgePartitionsAfterRemove(this.sqlgGraph1);

        p1.remove();
        this.sqlgGraph.tx().commit();
        Thread.sleep(2000);
        assert_edgePartitionsAfterRemove2(this.sqlgGraph);
        assert_edgePartitionsAfterRemove2(this.sqlgGraph1);

    }

    private void assert_edgePartitionsAfterRemove(SqlgGraph sqlgGraph1) {
        EdgeLabel edgeLabel1 = sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").get();
        Optional<Partition> optionalPartition = edgeLabel1.getPartition("int2");
        Assert.assertTrue(optionalPartition.isPresent());
        Partition p = optionalPartition.get();
        optionalPartition = p.getPartition("int22");
        Assert.assertTrue(!optionalPartition.isPresent());
        Assert.assertEquals(10, sqlgGraph1.topology().V().hasLabel("sqlg_schema.partition").count().next(), 0);
    }

    private void assert_edgePartitionsAfterRemove2(SqlgGraph sqlgGraph1) {
        EdgeLabel edgeLabel1 = sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").get();
        Optional<Partition> optionalPartition = edgeLabel1.getPartition("int1");
        Assert.assertFalse(optionalPartition.isPresent());
        Assert.assertEquals(3, sqlgGraph1.topology().V().hasLabel("sqlg_schema.partition").count().next(), 0);
    }

    private void assert_edgePartitions(SqlgGraph sqlgGraph1) {
        List<Edge> edges;VertexLabel vertexLabel = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").get();
        EdgeLabel edgeLabel = vertexLabel.getOutEdgeLabel("ab").get();

        Map<String, Partition> partitions = edgeLabel.getPartitions();
        Assert.assertEquals(2, partitions.size());
        Assert.assertTrue(partitions.containsKey("int1"));
        Partition pint1 = partitions.get("int1");
        Assert.assertEquals(2, pint1.getPartitions().size());
        Assert.assertTrue(pint1.getPartitions().containsKey("int11"));
        Partition pint11 = pint1.getPartitions().get("int11");
        Assert.assertEquals(2, pint11.getPartitions().size());
        Assert.assertTrue(pint11.getPartitions().containsKey("int111"));
        Partition pint111 = pint11.getPartitions().get("int111");
        Assert.assertEquals(0, pint111.getPartitions().size());
        Assert.assertTrue(pint11.getPartitions().containsKey("int112"));
        Partition pint112 = pint11.getPartitions().get("int112");
        Assert.assertEquals(0, pint112.getPartitions().size());


        Assert.assertTrue(pint1.getPartitions().containsKey("int12"));
        Partition pint12 = pint1.getPartitions().get("int12");
        Assert.assertEquals(2, pint12.getPartitions().size());
        Assert.assertTrue(pint12.getPartitions().containsKey("int121"));
        Partition pint121 = pint12.getPartitions().get("int121");
        Assert.assertEquals(0, pint121.getPartitions().size());
        Assert.assertTrue(pint12.getPartitions().containsKey("int122"));
        Partition pint122 = pint12.getPartitions().get("int122");
        Assert.assertEquals(0, pint122.getPartitions().size());

        Assert.assertTrue(partitions.containsKey("int2"));
        Partition pint2 = partitions.get("int2");
        Assert.assertEquals(2, pint2.getPartitions().size());
        Assert.assertTrue(pint2.getPartitions().containsKey("int21"));
        Partition pint21 = pint2.getPartitions().get("int21");
        Assert.assertEquals(1, pint21.getPartitions().size());
        Assert.assertTrue(pint21.getPartitions().containsKey("int221"));
        Partition pint221 = pint21.getPartitions().get("int221");
        Assert.assertEquals(0, pint221.getPartitions().size());

        Assert.assertTrue(pint2.getPartitions().containsKey("int22"));
        Partition pint22 = pint2.getPartitions().get("int22");
        Assert.assertEquals(1, pint22.getPartitions().size());
        Assert.assertTrue(pint22.getPartitions().containsKey("int222"));
        Partition pint222 = pint22.getPartitions().get("int222");
        Assert.assertEquals(0, pint222.getPartitions().size());

        Assert.assertEquals(12, sqlgGraph1.topology().V().hasLabel("sqlg_schema.partition").count().next(), 0);
        edges = sqlgGraph1.traversal().E().hasLabel("ab").has("int1", 1).has("int2", 1).has("int3", 1).toList();
        Assert.assertEquals(1, edges.size());
        edges = sqlgGraph1.traversal().E().hasLabel("ab").has("int1", 9).has("int2", 9).has("int3", 9).toList();
        Assert.assertEquals(1, edges.size());
        sqlgGraph1.tx().rollback();
    }

    @Test
    public void testSubSubPartitionList() throws InterruptedException {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensurePartitionedVertexLabelExist(
                "A",
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid", "int1", "int2", "int3")),
                PartitionType.LIST,
                "int1");
        Partition p1 = a.ensureListPartitionWithSubPartitionExists("int1", "1,2,3,4,5", PartitionType.LIST, "int2");
        Partition p2 = a.ensureListPartitionWithSubPartitionExists("int2", "6,7,8,9,10", PartitionType.LIST, "int2");

        Partition p1_1 = p1.ensureListPartitionWithSubPartitionExists("int11", "1,2,3,4,5", PartitionType.LIST, "int3");
        Partition p1_2 = p1.ensureListPartitionWithSubPartitionExists("int12", "6,7,8,9,10", PartitionType.LIST, "int3");
        Partition p2_1 = p2.ensureListPartitionWithSubPartitionExists("int21", "1,2,3,4,5", PartitionType.LIST, "int3");
        Partition p2_2 = p2.ensureListPartitionWithSubPartitionExists("int22", "6,7,8,9,10", PartitionType.LIST, "int3");

        p1_1.ensureListPartitionExists("int111", "1,2,3,4,5");
        p1_1.ensureListPartitionExists("int112", "6,7,8,9,10");
        p1_2.ensureListPartitionExists("int121", "1,2,3,4,5");
        p1_2.ensureListPartitionExists("int122", "6,7,8,9,10");
        p2_1.ensureListPartitionExists("int211", "1,2,3,4,5");
        p2_1.ensureListPartitionExists("int212", "6,7,8,9,10");
        p2_2.ensureListPartitionExists("int221", "1,2,3,4,5");
        p2_2.ensureListPartitionExists("int222", "6,7,8,9,10");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "int1", 1, "int2", 1, "int3", 1);
        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "int1", 5, "int2", 5, "int3", 5);

        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .has("int1", 1)
                .has("int2", 1)
                .has("int3", 1)
                .toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .has("int1", 5)
                .has("int2", 5)
                .has("int3", 5)
                .toList();
        Assert.assertEquals(1, vertices.size());

        Thread.sleep(1000);
        a = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get();
        Optional<Partition> int222 = a.getPartition("int222");
        Assert.assertTrue(int222.isPresent());
        a = this.sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").get();
        int222 = a.getPartition("int222");
        Assert.assertTrue(int222.isPresent());
    }

    @Test
    public void testEdgeSubSubPartitionList() throws Exception {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensureVertexLabelExist("A");
        VertexLabel b = publicSchema.ensureVertexLabelExist("B");
        EdgeLabel ab = a.ensurePartitionedEdgeLabelExist(
                "ab",
                b,
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid", "int1", "int2", "int3")),
                PartitionType.LIST,
                "int1");

        Partition p1 = ab.ensureListPartitionWithSubPartitionExists("int1", "1,2,3,4,5", PartitionType.LIST, "int2");
        Partition p2 = ab.ensureListPartitionWithSubPartitionExists("int2", "6,7,8,9,10", PartitionType.LIST, "int2");

        Partition p11 = p1.ensureListPartitionWithSubPartitionExists("int11", "1,2,3,4,5", PartitionType.LIST, "int3");
        Partition p12 = p1.ensureListPartitionWithSubPartitionExists("int12", "6,7,8,9,10", PartitionType.LIST, "int3");
        Partition p21 = p2.ensureListPartitionWithSubPartitionExists("int21", "1,2,3,4,5", PartitionType.LIST, "int3");
        Partition p22 = p2.ensureListPartitionWithSubPartitionExists("int22", "6,7,8,9,10", PartitionType.LIST, "int3");

        p11.ensureListPartitionExists("int111", "1,2,3,4,5");
        p11.ensureListPartitionExists("int112", "6,7,8,9,10");
        p12.ensureListPartitionExists("int121", "1,2,3,4,5");
        p12.ensureListPartitionExists("int122", "6,7,8,9,10");
        p21.ensureListPartitionExists("int221", "1,2,3,4,5");
        p22.ensureListPartitionExists("int222", "6,7,8,9,10");

        this.sqlgGraph.tx().commit();

        for (int i = 1; i < 10; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
            a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString(), "int1", i, "int2", i, "int3", i);
        }
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().toList();
        Assert.assertEquals(18, vertices.size());
        List<Edge> edges = this.sqlgGraph.traversal().E().toList();
        Assert.assertEquals(9, edges.size());
        vertices = this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .outE("ab")
                .has("int1", 1)
                .has("int2", 1)
                .has("int3", 1)
                .otherV()
                .toList();
        Assert.assertEquals(1, vertices.size());

        Thread.sleep(1000);
        ab = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get();
        Optional<Partition> p = ab.getPartition("int222");
        Assert.assertTrue(p.isPresent());
        ab = this.sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").get();
        p = ab.getPartition("int222");
        Assert.assertTrue(p.isPresent());

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
        }
    }
}
