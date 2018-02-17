package org.umlg.sqlg.test.index;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/01/28
 */
public class TestIndexOnPartition extends BaseTest {

    @Before
    public void before() throws Exception {
        super.before();
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsPartitioning());
    }

    @Test
    public void testCreateIndexOnPartitionedVertexLabelBeforeCreatingPartitions() {
        Schema schema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel vertexLabel = schema.ensurePartitionedVertexLabelExist("A",
                new HashMap<String, PropertyType>() {{
                    put("int1", PropertyType.INTEGER);
                }},
                PartitionType.RANGE,
                "int1");
        PropertyColumn propertyColumn = vertexLabel.getProperty("int1").get();
        Index index = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumn));
        vertexLabel.ensureRangePartitionExists("int1", "1", "5");

        schema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        vertexLabel = schema.ensurePartitionedVertexLabelExist("B",
                new HashMap<String, PropertyType>() {{
                    put("int1", PropertyType.INTEGER);
                }},
                PartitionType.RANGE,
                "int1");
        propertyColumn = vertexLabel.getProperty("int1").get();
        index = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumn));
        vertexLabel.ensureRangePartitionExists("int1", "0", "100");
        vertexLabel.ensureRangePartitionExists("int2", "100", "200");
        vertexLabel.ensureRangePartitionExists("int3", "200", "300");
        vertexLabel.ensureRangePartitionExists("int4", "300", "400");
        vertexLabel.ensureRangePartitionExists("int5", "400", "500");
        vertexLabel.ensureRangePartitionExists("int6", "500", "600");
        vertexLabel.ensureRangePartitionExists("int7", "600", "700");
        vertexLabel.ensureRangePartitionExists("int8", "700", "800");
        vertexLabel.ensureRangePartitionExists("int9", "800", "900");
        vertexLabel.ensureRangePartitionExists("int10", "900", "1000");
        this.sqlgGraph.tx().commit();

        for (int i = 0; i < 1000; i++) {
            this.sqlgGraph.addVertex(T.label, "B.B", "int1", i);
        }
        this.sqlgGraph.tx().commit();

        Map<String, Index> indexMap = this.sqlgGraph.getTopology().getSchema("B").get().getVertexLabel("B").get().getIndexes();
        Assert.assertEquals(1, indexMap.size());
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            indexMap = sqlgGraph1.getTopology().getSchema("B").get().getVertexLabel("B").get().getIndexes();
            Assert.assertEquals(1, indexMap.size());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

//    @Test
//    public void testCreateIndexOnPartitionedVertexLabelAfterCreatingPartitions() {
//        Schema schema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
//        VertexLabel vertexLabel = schema.ensurePartitionedVertexLabelExist("A",
//                new HashMap<String, PropertyType>() {{
//                    put("int1", PropertyType.INTEGER);
//                }},
//                PartitionType.RANGE,
//                "int1");
//        vertexLabel.ensureRangePartitionExists("int1", "1", "5");
//
//        schema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
//        vertexLabel = schema.ensurePartitionedVertexLabelExist("B",
//                new HashMap<String, PropertyType>() {{
//                    put("int1", PropertyType.INTEGER);
//                }},
//                PartitionType.RANGE,
//                "int1");
//        vertexLabel.ensureRangePartitionExists("int1", "0", "100");
//        vertexLabel.ensureRangePartitionExists("int2", "100", "200");
//        vertexLabel.ensureRangePartitionExists("int3", "200", "300");
//        vertexLabel.ensureRangePartitionExists("int4", "300", "400");
//        vertexLabel.ensureRangePartitionExists("int5", "400", "500");
//        vertexLabel.ensureRangePartitionExists("int6", "500", "600");
//        vertexLabel.ensureRangePartitionExists("int7", "600", "700");
//        vertexLabel.ensureRangePartitionExists("int8", "700", "800");
//        vertexLabel.ensureRangePartitionExists("int9", "800", "900");
//        vertexLabel.ensureRangePartitionExists("int10", "900", "1000");
//        this.sqlgGraph.tx().commit();
//
//        for (int i = 0; i < 1000; i++) {
//            this.sqlgGraph.addVertex(T.label, "B.B", "int1", i);
//        }
//        this.sqlgGraph.tx().commit();
//
//        PropertyColumn propertyColumn = vertexLabel.getProperty("int1").get();
//        Index index = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumn));
//        this.sqlgGraph.tx().commit();
//
//        Map<String, Index> indexMap = this.sqlgGraph.getTopology().getSchema("B").get().getVertexLabel("B").get().getIndexes();
//        Assert.assertEquals(1, indexMap.size());
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            indexMap = sqlgGraph1.getTopology().getSchema("B").get().getVertexLabel("B").get().getIndexes();
//            Assert.assertEquals(1, indexMap.size());
//        } catch (Exception e) {
//            Assert.fail(e.getMessage());
//        }
//    }
//
//    @Test
//    public void testCreateIndexOnPartitionedEdgeLabelBeforeCreatingPartitions() {
//
//        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
//        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
//        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A");
//        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B");
//        EdgeLabel abEdgeLabel = aVertexLabel.ensurePartitionedEdgeLabelExist(
//                "ab",
//                bVertexLabel,
//                new HashMap<String, PropertyType>() {{
//                    put("int1", PropertyType.INTEGER);
//                }},
//                PartitionType.RANGE,
//                "int1");
//
//
//        PropertyColumn propertyColumn = abEdgeLabel.getProperty("int1").get();
//        Index index = abEdgeLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumn));
//        abEdgeLabel.ensureRangePartitionExists("int1", "0", "100");
//        abEdgeLabel.ensureRangePartitionExists("int2", "100", "200");
//        abEdgeLabel.ensureRangePartitionExists("int3", "200", "300");
//        abEdgeLabel.ensureRangePartitionExists("int4", "300", "400");
//        abEdgeLabel.ensureRangePartitionExists("int5", "400", "500");
//        abEdgeLabel.ensureRangePartitionExists("int6", "500", "600");
//        abEdgeLabel.ensureRangePartitionExists("int7", "600", "700");
//        abEdgeLabel.ensureRangePartitionExists("int8", "700", "800");
//        abEdgeLabel.ensureRangePartitionExists("int9", "800", "900");
//        abEdgeLabel.ensureRangePartitionExists("int10", "900", "1000");
//        this.sqlgGraph.tx().commit();
//
//        for (int i = 0; i < 1000; i++) {
//            Vertex a = this.sqlgGraph.addVertex(T.label, "A.A");
//            Vertex b = this.sqlgGraph.addVertex(T.label, "B.B");
//            a.addEdge("ab", b, "int1", i);
//        }
//        this.sqlgGraph.tx().commit();
//
//        Map<String, Index> indexMap = this.sqlgGraph.getTopology().getSchema("A").get().getEdgeLabel("ab").get().getIndexes();
//        Assert.assertEquals(1, indexMap.size());
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            indexMap = sqlgGraph1.getTopology().getSchema("A").get().getEdgeLabel("ab").get().getIndexes();
//            Assert.assertEquals(1, indexMap.size());
//        } catch (Exception e) {
//            Assert.fail(e.getMessage());
//        }
//
//        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A.A").outE("ab").has("int1", 100).otherV().count().next(), 0);
//    }
//
//    @Test
//    public void testCreateIndexOnPartitionedEdgeLabelAfterCreatingPartitions() {
//
//        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
//        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
//        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A");
//        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B");
//        EdgeLabel abEdgeLabel = aVertexLabel.ensurePartitionedEdgeLabelExist(
//                "ab",
//                bVertexLabel,
//                new HashMap<String, PropertyType>() {{
//                    put("int1", PropertyType.INTEGER);
//                }},
//                PartitionType.RANGE,
//                "int1");
//
//
//        PropertyColumn propertyColumn = abEdgeLabel.getProperty("int1").get();
//        abEdgeLabel.ensureRangePartitionExists("int1", "0", "100");
//        abEdgeLabel.ensureRangePartitionExists("int2", "100", "200");
//        abEdgeLabel.ensureRangePartitionExists("int3", "200", "300");
//        abEdgeLabel.ensureRangePartitionExists("int4", "300", "400");
//        abEdgeLabel.ensureRangePartitionExists("int5", "400", "500");
//        abEdgeLabel.ensureRangePartitionExists("int6", "500", "600");
//        abEdgeLabel.ensureRangePartitionExists("int7", "600", "700");
//        abEdgeLabel.ensureRangePartitionExists("int8", "700", "800");
//        abEdgeLabel.ensureRangePartitionExists("int9", "800", "900");
//        abEdgeLabel.ensureRangePartitionExists("int10", "900", "1000");
//        this.sqlgGraph.tx().commit();
//        Index index = abEdgeLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumn));
//        this.sqlgGraph.tx().commit();
//
//        for (int i = 0; i < 1000; i++) {
//            Vertex a = this.sqlgGraph.addVertex(T.label, "A.A");
//            Vertex b = this.sqlgGraph.addVertex(T.label, "B.B");
//            a.addEdge("ab", b, "int1", i);
//        }
//        this.sqlgGraph.tx().commit();
//
//        Map<String, Index> indexMap = this.sqlgGraph.getTopology().getSchema("A").get().getEdgeLabel("ab").get().getIndexes();
//        Assert.assertEquals(1, indexMap.size());
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            indexMap = sqlgGraph1.getTopology().getSchema("A").get().getEdgeLabel("ab").get().getIndexes();
//            Assert.assertEquals(1, indexMap.size());
//        } catch (Exception e) {
//            Assert.fail(e.getMessage());
//        }
//
//        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A.A").outE("ab").has("int1", 100).otherV().count().next(), 0);
//    }
//
//    @Test
//    public void testCreateIndexOnSubPartitionedVertexLabelBeforeCreatingPartitions() {
//        Schema schema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
//        VertexLabel vertexLabel = schema.ensurePartitionedVertexLabelExist("A",
//                new HashMap<String, PropertyType>() {{
//                    put("int1", PropertyType.INTEGER);
//                    put("int2", PropertyType.INTEGER);
//                    put("int3", PropertyType.INTEGER);
//                    put("int4", PropertyType.INTEGER);
//                }},
//                PartitionType.RANGE,
//                "int1");
//
//        PropertyColumn propertyColumnInt1 = vertexLabel.getProperty("int1").get();
//        Index index1 = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumnInt1));
//        PropertyColumn propertyColumnInt2 = vertexLabel.getProperty("int2").get();
//        Index index2 = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumnInt2));
//        PropertyColumn propertyColumnInt3 = vertexLabel.getProperty("int3").get();
//        Index index3 = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumnInt3));
//        PropertyColumn propertyColumnInt4 = vertexLabel.getProperty("int4").get();
//        Index index4 = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumnInt4));
//
//        Partition int1_1_5 = vertexLabel.ensureRangePartitionWithSubPartitionExists("int1_1_5", "1", "5", PartitionType.RANGE, "int2");
//        Partition int1_5_10 = vertexLabel.ensureRangePartitionWithSubPartitionExists("int1_5_10", "5", "10", PartitionType.RANGE, "int2");
//
//        Partition int1_int2_1_5 = int1_1_5.ensureRangePartitionWithSubPartitionExists("int1_int2_1_5", "1", "5", PartitionType.RANGE, "int2");
//        Partition int1_int2_5_10 = int1_1_5.ensureRangePartitionWithSubPartitionExists("int1_int2_5_10", "5", "10", PartitionType.RANGE, "int2");
//
//        Partition int1_int2_int3_1_5 = int1_int2_1_5.ensureRangePartitionWithSubPartitionExists("int1_int2_int3_1_5", "1", "5", PartitionType.RANGE, "int3");
//        Partition int1_int2_int3_5_10 = int1_int2_1_5.ensureRangePartitionWithSubPartitionExists("int1_int2_int3_5_10", "5", "10", PartitionType.RANGE, "int3");
//
//        int1_int2_int3_1_5.ensureRangePartitionExists("int1_int2_int3_int4_1_5", "1", "5");
//        int1_int2_int3_1_5.ensureRangePartitionExists("int1_int2_int3_int4_5_10", "5", "10");
//
//        this.sqlgGraph.tx().commit();
//
//        Assert.assertEquals(4, this.sqlgGraph.getTopology().getVertexLabel("A", "A").get().getIndexes().size());
//
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            Map<String, Partition> partitions = sqlgGraph1.getTopology().getSchema("A").get().getVertexLabel("A").get().getPartitions();
//            Assert.assertEquals(2, partitions.size());
//            Assert.assertTrue(partitions.containsKey("int1_1_5"));
//            Assert.assertTrue(partitions.containsKey("int1_5_10"));
//
//            Partition p = partitions.get("int1_1_5");
//            Assert.assertEquals(2, p.getPartitions().size());
//            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_1_5"));
//            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_5_10"));
//
//            p = p.getPartitions().get("int1_int2_1_5");
//            Assert.assertEquals(2, p.getPartitions().size());
//            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_int3_1_5"));
//            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_int3_5_10"));
//
//            p = p.getPartitions().get("int1_int2_int3_1_5");
//            Assert.assertEquals(2, p.getPartitions().size());
//            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_int3_int4_1_5"));
//            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_int3_int4_5_10"));
//
//            Assert.assertEquals(4, sqlgGraph1.getTopology().getSchema("A").get().getVertexLabel("A").get().getIndexes().size());
//        } catch (Exception e) {
//            Assert.fail(e.getMessage());
//        }
//    }
//
//    @Test
//    public void testCreateIndexOnSubPartitionedVertexLabelAfterCreatingPartitions() {
//        Schema schema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
//        VertexLabel vertexLabel = schema.ensurePartitionedVertexLabelExist("A",
//                new HashMap<String, PropertyType>() {{
//                    put("int1", PropertyType.INTEGER);
//                    put("int2", PropertyType.INTEGER);
//                    put("int3", PropertyType.INTEGER);
//                    put("int4", PropertyType.INTEGER);
//                }},
//                PartitionType.RANGE,
//                "int1");
//
//        Partition int1_1_5 = vertexLabel.ensureRangePartitionWithSubPartitionExists("int1_1_5", "1", "5", PartitionType.RANGE, "int2");
//        Partition int1_5_10 = vertexLabel.ensureRangePartitionWithSubPartitionExists("int1_5_10", "5", "10", PartitionType.RANGE, "int2");
//
//        Partition int1_int2_1_5 = int1_1_5.ensureRangePartitionWithSubPartitionExists("int1_int2_1_5", "1", "5", PartitionType.RANGE, "int2");
//        Partition int1_int2_5_10 = int1_1_5.ensureRangePartitionWithSubPartitionExists("int1_int2_5_10", "5", "10", PartitionType.RANGE, "int2");
//
//        Partition int1_int2_int3_1_5 = int1_int2_1_5.ensureRangePartitionWithSubPartitionExists("int1_int2_int3_1_5", "1", "5", PartitionType.RANGE, "int3");
//        Partition int1_int2_int3_5_10 = int1_int2_1_5.ensureRangePartitionWithSubPartitionExists("int1_int2_int3_5_10", "5", "10", PartitionType.RANGE, "int3");
//
//        int1_int2_int3_1_5.ensureRangePartitionExists("int1_int2_int3_int4_1_5", "1", "5");
//        int1_int2_int3_1_5.ensureRangePartitionExists("int1_int2_int3_int4_5_10", "5", "10");
//
//        this.sqlgGraph.tx().commit();
//
//        PropertyColumn propertyColumnInt1 = vertexLabel.getProperty("int1").get();
//        Index index1 = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumnInt1));
//        PropertyColumn propertyColumnInt2 = vertexLabel.getProperty("int2").get();
//        Index index2 = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumnInt2));
//        PropertyColumn propertyColumnInt3 = vertexLabel.getProperty("int3").get();
//        Index index3 = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumnInt3));
//        PropertyColumn propertyColumnInt4 = vertexLabel.getProperty("int4").get();
//        Index index4 = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumnInt4));
//        Assert.assertEquals(4, this.sqlgGraph.getTopology().getVertexLabel("A", "A").get().getIndexes().size());
//        this.sqlgGraph.tx().commit();
//
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            Map<String, Partition> partitions = sqlgGraph1.getTopology().getSchema("A").get().getVertexLabel("A").get().getPartitions();
//            Assert.assertEquals(2, partitions.size());
//            Assert.assertTrue(partitions.containsKey("int1_1_5"));
//            Assert.assertTrue(partitions.containsKey("int1_5_10"));
//
//            Partition p = partitions.get("int1_1_5");
//            Assert.assertEquals(2, p.getPartitions().size());
//            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_1_5"));
//            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_5_10"));
//
//            p = p.getPartitions().get("int1_int2_1_5");
//            Assert.assertEquals(2, p.getPartitions().size());
//            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_int3_1_5"));
//            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_int3_5_10"));
//
//            p = p.getPartitions().get("int1_int2_int3_1_5");
//            Assert.assertEquals(2, p.getPartitions().size());
//            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_int3_int4_1_5"));
//            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_int3_int4_5_10"));
//
//            Assert.assertEquals(4, sqlgGraph1.getTopology().getSchema("A").get().getVertexLabel("A").get().getIndexes().size());
//        } catch (Exception e) {
//            Assert.fail(e.getMessage());
//        }
//    }
//
//    @Test
//    public void testCreateIndexOnSubPartitionedVertexLabelAndEdgeBeforeCreatingPartitions() {
//
//        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
//        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
//        VertexLabel a = aSchema.ensurePartitionedVertexLabelExist(
//                "A",
//                new HashMap<String, PropertyType>() {{
//                    put("int1", PropertyType.INTEGER);
//                    put("int2", PropertyType.INTEGER);
//                    put("int3", PropertyType.INTEGER);
//                    put("int4", PropertyType.INTEGER);
//                }},
//                PartitionType.LIST,
//                "int1");
//        VertexLabel b = bSchema.ensurePartitionedVertexLabelExist(
//                "B",
//                new HashMap<String, PropertyType>() {{
//                    put("int1", PropertyType.INTEGER);
//                    put("int2", PropertyType.INTEGER);
//                    put("int3", PropertyType.INTEGER);
//                    put("int4", PropertyType.INTEGER);
//                }},
//                PartitionType.LIST,
//                "int1");
//
//        EdgeLabel ab = a.ensurePartitionedEdgeLabelExist(
//                "ab",
//                b,
//                new HashMap<String, PropertyType>() {{
//                    put("int1", PropertyType.INTEGER);
//                    put("int2", PropertyType.INTEGER);
//                    put("int3", PropertyType.INTEGER);
//                    put("int4", PropertyType.INTEGER);
//                }},
//                PartitionType.LIST,
//                "int1");
//
//        a.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(a.getProperty("int1").get()));
//        a.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(a.getProperty("int2").get()));
//        a.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(a.getProperty("int3").get()));
//        a.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(a.getProperty("int4").get()));
//
//        Partition aInt1_1 = a.ensureListPartitionWithSubPartitionExists("aInt1_1", "1", PartitionType.LIST, "int2");
//        Partition aInt1_2 = a.ensureListPartitionWithSubPartitionExists("aInt1_2", "2", PartitionType.LIST, "int2");
//        Partition bInt1_1 = b.ensureListPartitionWithSubPartitionExists("bInt1_1", "1", PartitionType.LIST, "int2");
//        Partition bInt1_2 = b.ensureListPartitionWithSubPartitionExists("bInt1_2", "2", PartitionType.LIST, "int2");
//
//        Partition aInt1_1_int2_1 = aInt1_1.ensureListPartitionWithSubPartitionExists("aInt_1_int2_1", "1", PartitionType.LIST, "int3");
//        Partition aInt1_1_int2_2 = aInt1_1.ensureListPartitionWithSubPartitionExists("aInt_1_int2_2", "2", PartitionType.LIST, "int3");
//
//        Partition aInt1__1_int2_int3_1 = aInt1_1_int2_1.ensureListPartitionWithSubPartitionExists("aInt1_1_int2_1_int3_1", "1", PartitionType.LIST, "int4");
//        Partition aInt1__1_int2_int3_2 = aInt1_1_int2_1.ensureListPartitionWithSubPartitionExists("aInt1_1_int2_1_int3_2", "2", PartitionType.LIST, "int4");
//
//        aInt1__1_int2_int3_1.ensureListPartitionExists("aInt1_1_int2_1_int3_1_int4_1", "1");
//        aInt1__1_int2_int3_1.ensureListPartitionExists("aInt1_1_int2_1_int3_1_int4_2", "2");
//
//        Partition bInt1_int2_1 = bInt1_1.ensureListPartitionWithSubPartitionExists("bInt1_int2_1", "1", PartitionType.LIST, "int3");
//        Partition bInt1_int2_2 = bInt1_1.ensureListPartitionWithSubPartitionExists("bInt1_int2_2", "2", PartitionType.LIST, "int3");
//
//        Partition bInt1_int2_int3_1 = bInt1_int2_1.ensureListPartitionWithSubPartitionExists("bInt1_int2_int3_1", "1", PartitionType.LIST, "int4");
//        Partition bInt1_int2_int3_2 = bInt1_int2_1.ensureListPartitionWithSubPartitionExists("bInt1_int2_int3_2", "2", PartitionType.LIST, "int4");
//
//        bInt1_int2_int3_1.ensureListPartitionExists("bInt1_int2_int3_int4_1", "1");
//        bInt1_int2_int3_1.ensureListPartitionExists("bInt1_int2_int3_int4_2", "2");
//
//        Partition abInt1_1 = ab.ensureListPartitionWithSubPartitionExists("abInt1_1", "1", PartitionType.LIST, "int2");
//        Partition abInt1_2 = ab.ensureListPartitionWithSubPartitionExists("abInt1_2", "2", PartitionType.LIST, "int2");
//
//        Partition abInt1_int2_1 = abInt1_1.ensureListPartitionWithSubPartitionExists("abInt1_int2_1", "1", PartitionType.LIST, "int3");
//        Partition abInt1_int2_2 = abInt1_1.ensureListPartitionWithSubPartitionExists("abInt1_int2_2", "2", PartitionType.LIST, "int3");
//
//        Partition abInt1_int2_int3_1 = abInt1_int2_1.ensureListPartitionWithSubPartitionExists("abInt1_int2_int3_1", "1", PartitionType.LIST, "int4");
//        Partition abInt1_int2_int3_2 = abInt1_int2_1.ensureListPartitionWithSubPartitionExists("abInt1_int2_int3_2", "2", PartitionType.LIST, "int4");
//
//        abInt1_int2_int3_1.ensureListPartitionExists("abInt1_int2_int3_int4_1", "1");
//        abInt1_int2_int3_1.ensureListPartitionExists("abInt1_int2_int3_int4_2", "2");
//
//
//        this.sqlgGraph.tx().commit();
//
//        for (int i = 0; i < 1_000; i++) {
//            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A", "int1", 1, "int2", 1, "int3", 1, "int4", 1);
//            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "int1", 1, "int2", 1, "int3", 1, "int4", 1);
//            a1.addEdge("ab", b1, "int1", 1, "int2", 1, "int3", 1, "int4", 1);
//            a1.addEdge("ab", b1, "int1", 1, "int2", 1, "int3", 1, "int4", 2);
//        }
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A.A", "int1", 1, "int2", 1, "int3", 1, "int4", 2);
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B.B", "int1", 1, "int2", 1, "int3", 1, "int4", 2);
//        a2.addEdge("ab", b2, "int1", 1, "int2", 1, "int3", 1, "int4", 1);
//        a2.addEdge("ab", b2, "int1", 1, "int2", 1, "int3", 1, "int4", 2);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal()
//                .V().hasLabel("A.A").has("int1", 1)
//                .outE("ab").has("int1", 1)
//                .inV().has("int4", 2).toList();
//        Assert.assertEquals(2, vertices.size());
//    }
}
