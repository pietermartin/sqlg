package org.umlg.sqlg.test.topology;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/01/26
 */
public class TestSubSubPartition extends BaseTest {

    @Test
    public void testVertexSubSubPartitionRange() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensurePartitionedVertexLabelExist(
                "A",
                new HashMap<String, PropertyType>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                PartitionType.RANGE,
                "int1");
        Partition p1 = a.ensureRangePartitionWithSubPartitionExists("int1", "1", "5", PartitionType.RANGE, "int2");
        Partition p2 = a.ensureRangePartitionWithSubPartitionExists("int2", "5", "10", PartitionType.RANGE, "int2");

        Partition p1_1 = p1.ensureRangePartitionWithSubPartitionExist("int11", "1", "5", PartitionType.RANGE, "int3");
        Partition p1_2 = p1.ensureRangePartitionWithSubPartitionExist("int12", "5", "10", PartitionType.RANGE, "int3");
        Partition p2_1 = p2.ensureRangePartitionWithSubPartitionExist("int21", "1", "5", PartitionType.RANGE, "int3");
        Partition p2_2 = p2.ensureRangePartitionWithSubPartitionExist("int22", "5", "10", PartitionType.RANGE, "int3");

        p1_1.ensureRangePartitionExist("int111", "1", "5");
        p1_1.ensureRangePartitionExist("int112", "5", "10");
        p1_2.ensureRangePartitionExist("int121", "1", "5");
        p1_2.ensureRangePartitionExist("int122", "5", "10");
        p2_1.ensureRangePartitionExist("int211", "1", "5");
        p2_1.ensureRangePartitionExist("int212", "5", "10");
        p2_2.ensureRangePartitionExist("int221", "1", "5");
        p2_2.ensureRangePartitionExist("int222", "5", "10");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.addVertex(T.label, "A", "int1", 1, "int2", 1, "int3", 1);
        this.sqlgGraph.addVertex(T.label, "A", "int1", 5, "int2", 5, "int3", 5);

        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("int1", 1).has("int2", 1).has("int3", 1).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("int1", 5).has("int2", 5).has("int3", 5).toList();
        Assert.assertEquals(1, vertices.size());

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {

            VertexLabel vertexLabel = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").get();
            Map<String, Partition> partitions = vertexLabel.getPartitions();
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
            Assert.assertEquals(2, pint21.getPartitions().size());
            Assert.assertTrue(pint21.getPartitions().containsKey("int211"));
            Partition pint211 = pint21.getPartitions().get("int211");
            Assert.assertEquals(0, pint211.getPartitions().size());
            Assert.assertTrue(pint21.getPartitions().containsKey("int212"));
            Partition pint212 = pint21.getPartitions().get("int212");
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
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        p2.remove();
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, a.getPartitions().size());
        Assert.assertEquals(7, sqlgGraph.topology().V().hasLabel("sqlg_schema.partition").count().next(), 0);
    }

    @Test
    public void testEdgeSubSubPartitionRange() throws Exception {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensureVertexLabelExist("A");
        VertexLabel b = publicSchema.ensureVertexLabelExist("B");
        EdgeLabel ab = a.ensurePartitionedEdgeLabelExist(
                "ab",
                b,
                new HashMap<String, PropertyType>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                PartitionType.RANGE,
                "int1");

        Partition p1 = ab.ensureRangePartitionWithSubPartitionExists("int1", "1", "5", PartitionType.RANGE, "int2");
        Partition p2 = ab.ensureRangePartitionWithSubPartitionExists("int2", "5", "10", PartitionType.RANGE, "int2");

        Partition p11 = p1.ensureRangePartitionWithSubPartitionExist("int11", "1", "5", PartitionType.RANGE, "int3");
        Partition p12 = p1.ensureRangePartitionWithSubPartitionExist("int12", "5", "10", PartitionType.RANGE, "int3");
        Partition p21 = p2.ensureRangePartitionWithSubPartitionExist("int21", "1", "5", PartitionType.RANGE, "int3");
        Partition p22 = p2.ensureRangePartitionWithSubPartitionExist("int22", "5", "10", PartitionType.RANGE, "int3");

        p11.ensureRangePartitionExist("int111", "1", "5");
        p11.ensureRangePartitionExist("int112", "5", "10");
        p12.ensureRangePartitionExist("int121", "1", "5");
        p12.ensureRangePartitionExist("int122", "5", "10");
        p21.ensureRangePartitionExist("int221", "1", "5");
        p22.ensureRangePartitionExist("int222", "5", "10");

        this.sqlgGraph.tx().commit();

        for (int i = 1; i < 10; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
            a1.addEdge("ab", b1, "int1", i, "int2", i, "int3", i);
        }
        this.sqlgGraph.tx().commit();

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

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {

            VertexLabel vertexLabel = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").get();
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
        }

        p22.remove();
        this.sqlgGraph.tx().commit();
        optionalPartition = edgeLabel1.getPartition("int2");
        Assert.assertTrue(optionalPartition.isPresent());
        p = optionalPartition.get();
        optionalPartition = p.getPartition("int22");
        Assert.assertTrue(!optionalPartition.isPresent());
        Assert.assertEquals(10, sqlgGraph.topology().V().hasLabel("sqlg_schema.partition").count().next(), 0);
    }

    @Test
    public void testSubSubPartitionList() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensurePartitionedVertexLabelExist(
                "A",
                new HashMap<String, PropertyType>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                PartitionType.LIST,
                "int1");
        Partition p1 = a.ensureListPartitionWithSubPartitionExists("int1", "1,2,3,4,5", PartitionType.LIST, "int2");
        Partition p2 = a.ensureListPartitionWithSubPartitionExists("int2", "6,7,8,9,10", PartitionType.LIST, "int2");

        Partition p1_1 = p1.ensureListPartitionWithSubPartitionExist("int11", "1,2,3,4,5", PartitionType.LIST, "int3");
        Partition p1_2 = p1.ensureListPartitionWithSubPartitionExist("int12", "6,7,8,9,10", PartitionType.LIST, "int3");
        Partition p2_1 = p2.ensureListPartitionWithSubPartitionExist("int21", "1,2,3,4,5", PartitionType.LIST, "int3");
        Partition p2_2 = p2.ensureListPartitionWithSubPartitionExist("int22", "6,7,8,9,10", PartitionType.LIST, "int3");

        p1_1.ensureListPartitionExist("int111", "1,2,3,4,5");
        p1_1.ensureListPartitionExist("int112", "6,7,8,9,10");
        p1_2.ensureListPartitionExist("int121", "1,2,3,4,5");
        p1_2.ensureListPartitionExist("int122", "6,7,8,9,10");
        p2_1.ensureListPartitionExist("int211", "1,2,3,4,5");
        p2_1.ensureListPartitionExist("int212", "6,7,8,9,10");
        p2_2.ensureListPartitionExist("int221", "1,2,3,4,5");
        p2_2.ensureListPartitionExist("int222", "6,7,8,9,10");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.addVertex(T.label, "A", "int1", 1, "int2", 1, "int3", 1);
        this.sqlgGraph.addVertex(T.label, "A", "int1", 5, "int2", 5, "int3", 5);

        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("int1", 1).has("int2", 1).has("int3", 1).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("int1", 5).has("int2", 5).has("int3", 5).toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testEdgeSubSubPartitionList() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensureVertexLabelExist("A");
        VertexLabel b = publicSchema.ensureVertexLabelExist("B");
        EdgeLabel ab = a.ensurePartitionedEdgeLabelExist(
                "ab",
                b,
                new HashMap<String, PropertyType>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                PartitionType.LIST,
                "int1");

        Partition p1 = ab.ensureListPartitionWithSubPartitionExists("int1", "1,2,3,4,5", PartitionType.LIST, "int2");
        Partition p2 = ab.ensureListPartitionWithSubPartitionExists("int2", "6,7,8,9,10", PartitionType.LIST, "int2");

        Partition p11 = p1.ensureListPartitionWithSubPartitionExist("int11", "1,2,3,4,5", PartitionType.LIST, "int3");
        Partition p12 = p1.ensureListPartitionWithSubPartitionExist("int12", "6,7,8,9,10", PartitionType.LIST, "int3");
        Partition p21 = p2.ensureListPartitionWithSubPartitionExist("int21", "1,2,3,4,5", PartitionType.LIST, "int3");
        Partition p22 = p2.ensureListPartitionWithSubPartitionExist("int22", "6,7,8,9,10", PartitionType.LIST, "int3");

        p11.ensureListPartitionExist("int111", "1,2,3,4,5");
        p11.ensureListPartitionExist("int112", "6,7,8,9,10");
        p12.ensureListPartitionExist("int121", "1,2,3,4,5");
        p12.ensureListPartitionExist("int122", "6,7,8,9,10");
        p21.ensureListPartitionExist("int221", "1,2,3,4,5");
        p22.ensureListPartitionExist("int222", "6,7,8,9,10");

        this.sqlgGraph.tx().commit();

        for (int i = 1; i < 10; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
            a1.addEdge("ab", b1, "int1", i, "int2", i, "int3", i);
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
    }
}
