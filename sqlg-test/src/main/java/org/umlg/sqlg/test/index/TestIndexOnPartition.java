package org.umlg.sqlg.test.index;

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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/01/28
 */
@SuppressWarnings({"DuplicatedCode", "unused"})
public class TestIndexOnPartition extends BaseTest {

    @Before
    public void before() throws Exception {
        super.before();
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsPartitioning());
    }

    @Test
    public void testCreateIndexOnPartitionedVertexLabelBeforeCreatingPartitions() {
        Schema schema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel vertexLabel = schema.ensurePartitionedVertexLabelExist(
                "A",
                new HashMap<>() {{
                    put("int1", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("int1")),
                PartitionType.RANGE,
                "int1");
        PropertyColumn propertyColumn = vertexLabel.getProperty("int1").get();
        Index index = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumn));
        vertexLabel.ensureRangePartitionExists("int1", "1", "5");

        schema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        vertexLabel = schema.ensurePartitionedVertexLabelExist(
                "B",
                new HashMap<>() {{
                    put("int1", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("int1")),
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

    @Test
    public void testCreateIndexOnPartitionedVertexLabelAfterCreatingPartitions() {
        Schema schema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel vertexLabel = schema.ensurePartitionedVertexLabelExist(
                "A",
                new HashMap<>() {{
                    put("int1", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("int1")),
                PartitionType.RANGE,
                "int1");
        vertexLabel.ensureRangePartitionExists("int1", "1", "5");

        schema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        vertexLabel = schema.ensurePartitionedVertexLabelExist(
                "B",
                new HashMap<>() {{
                    put("int1", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("int1")),
                PartitionType.RANGE,
                "int1");
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

        PropertyColumn propertyColumn = vertexLabel.getProperty("int1").get();
        Index index = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumn));
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

    @Test
    public void testCreateIndexOnPartitionedEdgeLabelBeforeCreatingPartitions() {

        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B");
        EdgeLabel abEdgeLabel = aVertexLabel.ensurePartitionedEdgeLabelExist(
                "ab",
                bVertexLabel,
                new HashMap<>() {{
                    put("int1", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("int1")),
                PartitionType.RANGE,
                "int1");


        PropertyColumn propertyColumn = abEdgeLabel.getProperty("int1").get();
        Index index = abEdgeLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumn));
        abEdgeLabel.ensureRangePartitionExists("int1", "0", "100");
        abEdgeLabel.ensureRangePartitionExists("int2", "100", "200");
        abEdgeLabel.ensureRangePartitionExists("int3", "200", "300");
        abEdgeLabel.ensureRangePartitionExists("int4", "300", "400");
        abEdgeLabel.ensureRangePartitionExists("int5", "400", "500");
        abEdgeLabel.ensureRangePartitionExists("int6", "500", "600");
        abEdgeLabel.ensureRangePartitionExists("int7", "600", "700");
        abEdgeLabel.ensureRangePartitionExists("int8", "700", "800");
        abEdgeLabel.ensureRangePartitionExists("int9", "800", "900");
        abEdgeLabel.ensureRangePartitionExists("int10", "900", "1000");
        this.sqlgGraph.tx().commit();

        for (int i = 0; i < 1000; i++) {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A.A");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B.B");
            a.addEdge("ab", b, "int1", i);
        }
        this.sqlgGraph.tx().commit();

        Map<String, Index> indexMap = this.sqlgGraph.getTopology().getSchema("A").get().getEdgeLabel("ab").get().getIndexes();
        Assert.assertEquals(1, indexMap.size());
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            indexMap = sqlgGraph1.getTopology().getSchema("A").get().getEdgeLabel("ab").get().getIndexes();
            Assert.assertEquals(1, indexMap.size());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A.A").outE("ab").has("int1", 100).otherV().count().next(), 0);
    }

    @Test
    public void testCreateIndexOnPartitionedEdgeLabelAfterCreatingPartitions() {

        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B");
        EdgeLabel abEdgeLabel = aVertexLabel.ensurePartitionedEdgeLabelExist(
                "ab",
                bVertexLabel,
                new HashMap<>() {{
                    put("int1", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("int1")),
                PartitionType.RANGE,
                "int1");


        PropertyColumn propertyColumn = abEdgeLabel.getProperty("int1").get();
        abEdgeLabel.ensureRangePartitionExists("int1", "0", "100");
        abEdgeLabel.ensureRangePartitionExists("int2", "100", "200");
        abEdgeLabel.ensureRangePartitionExists("int3", "200", "300");
        abEdgeLabel.ensureRangePartitionExists("int4", "300", "400");
        abEdgeLabel.ensureRangePartitionExists("int5", "400", "500");
        abEdgeLabel.ensureRangePartitionExists("int6", "500", "600");
        abEdgeLabel.ensureRangePartitionExists("int7", "600", "700");
        abEdgeLabel.ensureRangePartitionExists("int8", "700", "800");
        abEdgeLabel.ensureRangePartitionExists("int9", "800", "900");
        abEdgeLabel.ensureRangePartitionExists("int10", "900", "1000");
        this.sqlgGraph.tx().commit();
        Index index = abEdgeLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumn));
        this.sqlgGraph.tx().commit();

        for (int i = 0; i < 1000; i++) {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A.A");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B.B");
            a.addEdge("ab", b, "int1", i);
        }
        this.sqlgGraph.tx().commit();

        Map<String, Index> indexMap = this.sqlgGraph.getTopology().getSchema("A").get().getEdgeLabel("ab").get().getIndexes();
        Assert.assertEquals(1, indexMap.size());
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            indexMap = sqlgGraph1.getTopology().getSchema("A").get().getEdgeLabel("ab").get().getIndexes();
            Assert.assertEquals(1, indexMap.size());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A.A").outE("ab").has("int1", 100).otherV().count().next(), 0);
    }

    @Test
    public void testCreateIndexOnSubPartitionedVertexLabelBeforeCreatingPartitions() {
        Schema schema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel vertexLabel = schema.ensurePartitionedVertexLabelExist("A",
                new HashMap<>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                    put("int4", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("int1", "int2", "int3")),
                PartitionType.RANGE,
                "int1");

        PropertyColumn propertyColumnInt1 = vertexLabel.getProperty("int1").orElseThrow();
        Index index1 = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumnInt1));
        PropertyColumn propertyColumnInt2 = vertexLabel.getProperty("int2").orElseThrow();
        Index index2 = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumnInt2));
        PropertyColumn propertyColumnInt3 = vertexLabel.getProperty("int3").orElseThrow();
        Index index3 = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumnInt3));
        PropertyColumn propertyColumnInt4 = vertexLabel.getProperty("int4").orElseThrow();
        Index index4 = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumnInt4));

        Partition int1_1_5 = vertexLabel.ensureRangePartitionWithSubPartitionExists("int1_1_5", "1", "5", PartitionType.RANGE, "int2");
        Partition int1_5_10 = vertexLabel.ensureRangePartitionWithSubPartitionExists("int1_5_10", "5", "10", PartitionType.RANGE, "int2");

        Partition int1_int2_1_5 = int1_1_5.ensureRangePartitionWithSubPartitionExists("int1_int2_1_5", "1", "5", PartitionType.RANGE, "int2");
        Partition int1_int2_5_10 = int1_1_5.ensureRangePartitionWithSubPartitionExists("int1_int2_5_10", "5", "10", PartitionType.RANGE, "int2");

        Partition int1_int2_int3_1_5 = int1_int2_1_5.ensureRangePartitionWithSubPartitionExists("int1_int2_int3_1_5", "1", "5", PartitionType.RANGE, "int3");
        Partition int1_int2_int3_5_10 = int1_int2_1_5.ensureRangePartitionWithSubPartitionExists("int1_int2_int3_5_10", "5", "10", PartitionType.RANGE, "int3");

        int1_int2_int3_1_5.ensureRangePartitionExists("int1_int2_int3_int4_1_5", "1", "5");
        int1_int2_int3_1_5.ensureRangePartitionExists("int1_int2_int3_int4_5_10", "5", "10");

        this.sqlgGraph.tx().commit();

        Assert.assertEquals(4, this.sqlgGraph.getTopology().getVertexLabel("A", "A").get().getIndexes().size());

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Map<String, Partition> partitions = sqlgGraph1.getTopology().getSchema("A").get().getVertexLabel("A").get().getPartitions();
            Assert.assertEquals(2, partitions.size());
            Assert.assertTrue(partitions.containsKey("int1_1_5"));
            Assert.assertTrue(partitions.containsKey("int1_5_10"));

            Partition p = partitions.get("int1_1_5");
            Assert.assertEquals(2, p.getPartitions().size());
            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_1_5"));
            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_5_10"));

            p = p.getPartitions().get("int1_int2_1_5");
            Assert.assertEquals(2, p.getPartitions().size());
            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_int3_1_5"));
            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_int3_5_10"));

            p = p.getPartitions().get("int1_int2_int3_1_5");
            Assert.assertEquals(2, p.getPartitions().size());
            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_int3_int4_1_5"));
            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_int3_int4_5_10"));

            Assert.assertEquals(4, sqlgGraph1.getTopology().getSchema("A").get().getVertexLabel("A").get().getIndexes().size());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCreateIndexOnSubPartitionedVertexLabelAfterCreatingPartitions() {
        Schema schema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel vertexLabel = schema.ensurePartitionedVertexLabelExist("A",
                new HashMap<>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                    put("int4", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("int1", "int2", "int3")),
                PartitionType.RANGE,
                "int1");

        Partition int1_1_5 = vertexLabel.ensureRangePartitionWithSubPartitionExists("int1_1_5", "1", "5", PartitionType.RANGE, "int2");
        Partition int1_5_10 = vertexLabel.ensureRangePartitionWithSubPartitionExists("int1_5_10", "5", "10", PartitionType.RANGE, "int2");

        Partition int1_int2_1_5 = int1_1_5.ensureRangePartitionWithSubPartitionExists("int1_int2_1_5", "1", "5", PartitionType.RANGE, "int2");
        Partition int1_int2_5_10 = int1_1_5.ensureRangePartitionWithSubPartitionExists("int1_int2_5_10", "5", "10", PartitionType.RANGE, "int2");

        Partition int1_int2_int3_1_5 = int1_int2_1_5.ensureRangePartitionWithSubPartitionExists("int1_int2_int3_1_5", "1", "5", PartitionType.RANGE, "int3");
        Partition int1_int2_int3_5_10 = int1_int2_1_5.ensureRangePartitionWithSubPartitionExists("int1_int2_int3_5_10", "5", "10", PartitionType.RANGE, "int3");

        int1_int2_int3_1_5.ensureRangePartitionExists("int1_int2_int3_int4_1_5", "1", "5");
        int1_int2_int3_1_5.ensureRangePartitionExists("int1_int2_int3_int4_5_10", "5", "10");

        this.sqlgGraph.tx().commit();

        PropertyColumn propertyColumnInt1 = vertexLabel.getProperty("int1").get();
        Index index1 = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumnInt1));
        PropertyColumn propertyColumnInt2 = vertexLabel.getProperty("int2").get();
        Index index2 = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumnInt2));
        PropertyColumn propertyColumnInt3 = vertexLabel.getProperty("int3").get();
        Index index3 = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumnInt3));
        PropertyColumn propertyColumnInt4 = vertexLabel.getProperty("int4").get();
        Index index4 = vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumnInt4));
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getVertexLabel("A", "A").get().getIndexes().size());
        this.sqlgGraph.tx().commit();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Map<String, Partition> partitions = sqlgGraph1.getTopology().getSchema("A").get().getVertexLabel("A").get().getPartitions();
            Assert.assertEquals(2, partitions.size());
            Assert.assertTrue(partitions.containsKey("int1_1_5"));
            Assert.assertTrue(partitions.containsKey("int1_5_10"));

            Partition p = partitions.get("int1_1_5");
            Assert.assertEquals(2, p.getPartitions().size());
            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_1_5"));
            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_5_10"));

            p = p.getPartitions().get("int1_int2_1_5");
            Assert.assertEquals(2, p.getPartitions().size());
            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_int3_1_5"));
            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_int3_5_10"));

            p = p.getPartitions().get("int1_int2_int3_1_5");
            Assert.assertEquals(2, p.getPartitions().size());
            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_int3_int4_1_5"));
            Assert.assertTrue(p.getPartitions().containsKey("int1_int2_int3_int4_5_10"));

            Assert.assertEquals(4, sqlgGraph1.getTopology().getSchema("A").get().getVertexLabel("A").get().getIndexes().size());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCreateIndexOnSubPartitionedVertexLabelAndEdgeBeforeCreatingPartitions() {

        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        VertexLabel a = aSchema.ensurePartitionedVertexLabelExist(
                "A",
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                    put("int4", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid")),
                PartitionType.LIST,
                "int1", false);
        VertexLabel b = bSchema.ensurePartitionedVertexLabelExist(
                "B",
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                    put("int4", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid")),
                PartitionType.LIST,
                "int1", false);

        EdgeLabel ab = a.ensurePartitionedEdgeLabelExist(
                "ab",
                b,
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                    put("int4", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid", "int1", "int2", "int3", "int4")),
                PartitionType.LIST,
                "int1");

        a.ensureIndexExists(IndexType.NON_UNIQUE, Collections.singletonList(a.getProperty("int1").orElseThrow()));
        a.ensureIndexExists(IndexType.NON_UNIQUE, Collections.singletonList(a.getProperty("int2").orElseThrow()));
        a.ensureIndexExists(IndexType.NON_UNIQUE, Collections.singletonList(a.getProperty("int3").orElseThrow()));
        a.ensureIndexExists(IndexType.NON_UNIQUE, Collections.singletonList(a.getProperty("int4").orElseThrow()));

        Partition aInt1_1 = a.ensureListPartitionWithSubPartitionExists("aInt1_1", "1", PartitionType.LIST, "int2");
        Partition aInt1_2 = a.ensureListPartitionWithSubPartitionExists("aInt1_2", "2", PartitionType.LIST, "int2");
        Partition bInt1_1 = b.ensureListPartitionWithSubPartitionExists("bInt1_1", "1", PartitionType.LIST, "int2");
        Partition bInt1_2 = b.ensureListPartitionWithSubPartitionExists("bInt1_2", "2", PartitionType.LIST, "int2");

        Partition aInt1_1_int2_1 = aInt1_1.ensureListPartitionWithSubPartitionExists("aInt_1_int2_1", "1", PartitionType.LIST, "int3");
        Partition aInt1_1_int2_2 = aInt1_1.ensureListPartitionWithSubPartitionExists("aInt_1_int2_2", "2", PartitionType.LIST, "int3");

        Partition aInt1__1_int2_int3_1 = aInt1_1_int2_1.ensureListPartitionWithSubPartitionExists("aInt1_1_int2_1_int3_1", "1", PartitionType.LIST, "int4");
        Partition aInt1__1_int2_int3_2 = aInt1_1_int2_1.ensureListPartitionWithSubPartitionExists("aInt1_1_int2_1_int3_2", "2", PartitionType.LIST, "int4");

        aInt1__1_int2_int3_1.ensureListPartitionExists("aInt1_1_int2_1_int3_1_int4_1", "1");
        aInt1__1_int2_int3_1.ensureListPartitionExists("aInt1_1_int2_1_int3_1_int4_2", "2");

        Partition bInt1_int2_1 = bInt1_1.ensureListPartitionWithSubPartitionExists("bInt1_int2_1", "1", PartitionType.LIST, "int3");
        Partition bInt1_int2_2 = bInt1_1.ensureListPartitionWithSubPartitionExists("bInt1_int2_2", "2", PartitionType.LIST, "int3");

        Partition bInt1_int2_int3_1 = bInt1_int2_1.ensureListPartitionWithSubPartitionExists("bInt1_int2_int3_1", "1", PartitionType.LIST, "int4");
        Partition bInt1_int2_int3_2 = bInt1_int2_1.ensureListPartitionWithSubPartitionExists("bInt1_int2_int3_2", "2", PartitionType.LIST, "int4");

        bInt1_int2_int3_1.ensureListPartitionExists("bInt1_int2_int3_int4_1", "1");
        bInt1_int2_int3_1.ensureListPartitionExists("bInt1_int2_int3_int4_2", "2");

        Partition abInt1_1 = ab.ensureListPartitionWithSubPartitionExists("abInt1_1", "1", PartitionType.LIST, "int2");
        Partition abInt1_2 = ab.ensureListPartitionWithSubPartitionExists("abInt1_2", "2", PartitionType.LIST, "int2");

        Partition abInt1_int2_1 = abInt1_1.ensureListPartitionWithSubPartitionExists("abInt1_int2_1", "1", PartitionType.LIST, "int3");
        Partition abInt1_int2_2 = abInt1_1.ensureListPartitionWithSubPartitionExists("abInt1_int2_2", "2", PartitionType.LIST, "int3");

        Partition abInt1_int2_int3_1 = abInt1_int2_1.ensureListPartitionWithSubPartitionExists("abInt1_int2_int3_1", "1", PartitionType.LIST, "int4");
        Partition abInt1_int2_int3_2 = abInt1_int2_1.ensureListPartitionWithSubPartitionExists("abInt1_int2_int3_2", "2", PartitionType.LIST, "int4");

        abInt1_int2_int3_1.ensureListPartitionExists("abInt1_int2_int3_int4_1", "1");
        abInt1_int2_int3_1.ensureListPartitionExists("abInt1_int2_int3_int4_2", "2");


        this.sqlgGraph.tx().commit();

        for (int i = 0; i < 1_000; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A", "uid", UUID.randomUUID().toString(), "int1", 1, "int2", 1, "int3", 1, "int4", 1);
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "uid", UUID.randomUUID().toString(), "int1", 1, "int2", 1, "int3", 1, "int4", 1);
            a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString(), "int1", 1, "int2", 1, "int3", 1, "int4", 1);
            a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString(), "int1", 1, "int2", 1, "int3", 1, "int4", 2);
        }
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A.A", "uid", UUID.randomUUID().toString(), "int1", 1, "int2", 1, "int3", 1, "int4", 2);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B.B", "uid", UUID.randomUUID().toString(), "int1", 1, "int2", 1, "int3", 1, "int4", 2);
        a2.addEdge("ab", b2, "uid", UUID.randomUUID().toString(), "int1", 1, "int2", 1, "int3", 1, "int4", 1);
        a2.addEdge("ab", b2, "uid", UUID.randomUUID().toString(), "int1", 1, "int2", 1, "int3", 1, "int4", 2);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V().hasLabel("A.A").has("int1", 1)
                .outE("ab").has("int1", 1)
                .inV().has("int4", 2).toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testListPartitiongetsIndex() {
        String VIRTUAL_GROUP_PARENT_NAME = "virtualGroupParentName";
        String VIRTUAL_GROUP_NAME = "virtualGroupName";
        String VIRTUAL_GROUP_ID = "virtualGroupId";
        String vendorTechnology = "vendorTechnology";
        String etlElementName = "etlElementName";
        String cmUid = "cmUid";
        String internal_cm_name = "internal_cm_name";
        String IS_CALCULATED = "isCalculated";
        String IS_DELETED = "isDeleted";
        String VIRTUAL_GROUP_ROOT_REAL_WORKSPACE_ELEMENT_LABEL = "VGReal";

        LinkedHashMap<String, PropertyType> attributeMap = new LinkedHashMap<>();
        attributeMap.put(VIRTUAL_GROUP_PARENT_NAME, PropertyType.STRING);
        attributeMap.put(VIRTUAL_GROUP_NAME, PropertyType.STRING);
        attributeMap.put(VIRTUAL_GROUP_ID, PropertyType.LONG);
        attributeMap.put(vendorTechnology, PropertyType.STRING);
        attributeMap.put(etlElementName, PropertyType.STRING);
        attributeMap.put(cmUid, PropertyType.STRING);
        attributeMap.put(internal_cm_name, PropertyType.STRING);
        attributeMap.put(IS_CALCULATED, PropertyType.BOOLEAN);
        attributeMap.put(IS_DELETED, PropertyType.BOOLEAN);

        VertexLabel virtualGroupRootRealWorkspaceElementVertexLabel = sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                VIRTUAL_GROUP_ROOT_REAL_WORKSPACE_ELEMENT_LABEL,
                attributeMap,
                ListOrderedSet.listOrderedSet(List.of(cmUid, VIRTUAL_GROUP_ID)),
                PartitionType.LIST,
                "\"" + VIRTUAL_GROUP_ID + "\""
        );

        //Unique index, element can only appear once per virtual group parent
        PropertyColumn cmUidPropertyColumn = virtualGroupRootRealWorkspaceElementVertexLabel.getProperty(cmUid).orElseThrow();
        virtualGroupRootRealWorkspaceElementVertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(cmUidPropertyColumn));

        this.sqlgGraph.tx().commit();

        int virtualGroupId = 1;
        VertexLabel realVertexLabel = sqlgGraph.getTopology().getPublicSchema().getVertexLabel(VIRTUAL_GROUP_ROOT_REAL_WORKSPACE_ELEMENT_LABEL).orElseThrow();
        Partition partition = realVertexLabel.ensureListPartitionExists(
                "VirtualGroupRootReal_" + virtualGroupId,
                "'" + virtualGroupId + "'"
        );

        this.sqlgGraph.tx().commit();

        realVertexLabel = sqlgGraph.getTopology().getPublicSchema().getVertexLabel(VIRTUAL_GROUP_ROOT_REAL_WORKSPACE_ELEMENT_LABEL).orElseThrow();
        Optional<Partition> partitionOptional = realVertexLabel.getPartition(partition.getName());
        Assert.assertTrue(partitionOptional.isPresent());
        Map<String, Index> indexMap = realVertexLabel.getIndexes();
        Assert.assertEquals(1, indexMap.size());

        //Check if the index is being used
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            if (this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
                ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"public\".\"V_" + VIRTUAL_GROUP_ROOT_REAL_WORKSPACE_ELEMENT_LABEL + "\" a WHERE a.\"" + cmUid + "\" = 'john'");
                Assert.assertTrue(rs.next());
                String result = rs.getString(1);
                System.out.println(result);
                Assert.assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testRangePartitiongetsIndex() {
        String VIRTUAL_GROUP_PARENT_NAME = "virtualGroupParentName";
        String VIRTUAL_GROUP_NAME = "virtualGroupName";
        String VIRTUAL_GROUP_ID = "virtualGroupId";
        String vendorTechnology = "vendorTechnology";
        String etlElementName = "etlElementName";
        String cmUid = "cmUid";
        String internal_cm_name = "internal_cm_name";
        String IS_CALCULATED = "isCalculated";
        String IS_DELETED = "isDeleted";
        String VIRTUAL_GROUP_ROOT_REAL_WORKSPACE_ELEMENT_LABEL = "VGReal";

        LinkedHashMap<String, PropertyType> attributeMap = new LinkedHashMap<>();
        attributeMap.put(VIRTUAL_GROUP_PARENT_NAME, PropertyType.STRING);
        attributeMap.put(VIRTUAL_GROUP_NAME, PropertyType.STRING);
        attributeMap.put(VIRTUAL_GROUP_ID, PropertyType.LONG);
        attributeMap.put(vendorTechnology, PropertyType.STRING);
        attributeMap.put(etlElementName, PropertyType.STRING);
        attributeMap.put(cmUid, PropertyType.STRING);
        attributeMap.put(internal_cm_name, PropertyType.STRING);
        attributeMap.put(IS_CALCULATED, PropertyType.BOOLEAN);
        attributeMap.put(IS_DELETED, PropertyType.BOOLEAN);

        VertexLabel virtualGroupRootRealWorkspaceElementVertexLabel = sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                VIRTUAL_GROUP_ROOT_REAL_WORKSPACE_ELEMENT_LABEL,
                attributeMap,
                ListOrderedSet.listOrderedSet(List.of(cmUid, VIRTUAL_GROUP_ID)),
                PartitionType.RANGE,
                "\"" + VIRTUAL_GROUP_ID + "\""
        );

        //Unique index, element can only appear once per virtual group parent
        PropertyColumn cmUidPropertyColumn = virtualGroupRootRealWorkspaceElementVertexLabel.getProperty(cmUid).orElseThrow();
        virtualGroupRootRealWorkspaceElementVertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(cmUidPropertyColumn));

        this.sqlgGraph.tx().commit();

        int virtualGroupId = 1;
        int virtualGroupIdTo = 2;
        VertexLabel realVertexLabel = sqlgGraph.getTopology().getPublicSchema().getVertexLabel(VIRTUAL_GROUP_ROOT_REAL_WORKSPACE_ELEMENT_LABEL).orElseThrow();
        Partition partition = realVertexLabel.ensureRangePartitionExists(
                "VirtualGroupRootReal_" + virtualGroupId,
                "'" + virtualGroupId + "'",
                "'" + virtualGroupIdTo + "'"
        );

        this.sqlgGraph.tx().commit();

        realVertexLabel = sqlgGraph.getTopology().getPublicSchema().getVertexLabel(VIRTUAL_GROUP_ROOT_REAL_WORKSPACE_ELEMENT_LABEL).orElseThrow();
        Optional<Partition> partitionOptional = realVertexLabel.getPartition(partition.getName());
        Assert.assertTrue(partitionOptional.isPresent());
        Map<String, Index> indexMap = realVertexLabel.getIndexes();
        Assert.assertEquals(1, indexMap.size());

        //Check if the index is being used
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            if (this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
                ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"public\".\"V_" + VIRTUAL_GROUP_ROOT_REAL_WORKSPACE_ELEMENT_LABEL + "\" a WHERE a.\"" + cmUid + "\" = 'john'");
                Assert.assertTrue(rs.next());
                String result = rs.getString(1);
                System.out.println(result);
                Assert.assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

}
