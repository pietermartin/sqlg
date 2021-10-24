package org.umlg.sqlg.test.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/01/13
 */
@SuppressWarnings({"UnusedAssignment", "unused", "DuplicatedCode"})
public class TestPartitioning extends BaseTest {

    @Before
    public void before() throws Exception {
        super.before();
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsPartitioning());
    }

    private enum TEST {
        TEST1,
        TEST2,
        TEST3
    }

    @Test
    public void testReloadVertexLabelWithPartitions2() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("int1", "int2")),
                PartitionType.RANGE,
                "int1,int2");
        a.ensureRangePartitionExists("part1", "1,1", "5,5");
        a.ensureRangePartitionExists("part2", "5,5", "10,10");
        this.sqlgGraph.tx().commit();

        String checkPrimaryKey = "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type\n" +
                "FROM   pg_index i\n" +
                "JOIN   pg_attribute a ON a.attrelid = i.indrelid\n" +
                "                     AND a.attnum = ANY(i.indkey)\n" +
                "WHERE  i.indrelid = '\"V_A\"'::regclass\n" +
                "AND    i.indisprimary;";
        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(checkPrimaryKey);
            List<Pair<String, String>> keys = new ArrayList<>();
            while (rs.next()) {
                String key = rs.getString("attname");
                String dataType = rs.getString("data_type");
                keys.add(Pair.of(key, dataType));
            }
            Assert.assertEquals(2, keys.size());
            Assert.assertEquals("int1", keys.get(0).getLeft());
            Assert.assertEquals("int2", keys.get(1).getLeft());
        } catch (SQLException throwables) {
            Assert.fail(throwables.getMessage());
        }

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            a = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow();
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
    public void testPartitionWithCompositeKeys() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.INTEGER);
                    put("uid2", PropertyType.LONG);
                    put("uid3", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid1", "uid2", "uid3")),
                PartitionType.LIST,
                "\"uid1\""
        );
        vertexLabel.ensureListPartitionExists("listPartition1", "'1'");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "uid1", 1, "uid2", 1L, "uid3", "halo1");
        this.sqlgGraph.addVertex(T.label, "A", "uid1", 1, "uid2", 2L, "uid3", "halo1");
        this.sqlgGraph.tx().commit();

        String checkPrimaryKey = "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type\n" +
                "FROM   pg_index i\n" +
                "JOIN   pg_attribute a ON a.attrelid = i.indrelid\n" +
                "                     AND a.attnum = ANY(i.indkey)\n" +
                "WHERE  i.indrelid = '\"V_A\"'::regclass\n" +
                "AND    i.indisprimary;";
        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(checkPrimaryKey);
            List<Pair<String, String>> keys = new ArrayList<>();
            while (rs.next()) {
                String key = rs.getString("attname");
                String dataType = rs.getString("data_type");
                keys.add(Pair.of(key, dataType));
            }
            Assert.assertEquals(3, keys.size());
            Assert.assertEquals("uid1", keys.get(0).getLeft());
            Assert.assertEquals("uid2", keys.get(1).getLeft());
            Assert.assertEquals("uid3", keys.get(2).getLeft());
        } catch (SQLException throwables) {
            Assert.fail(throwables.getMessage());
        }

        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            ListOrderedSet<String> identifiers = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getIdentifiers();
            Assert.assertEquals("uid1", identifiers.get(0));
            Assert.assertEquals("uid2", identifiers.get(1));
            Assert.assertEquals("uid3", identifiers.get(2));
        }
    }

    /**
     * This test had a bug, making it pass. It is not possible to define a LIST partition on multiple keys.
     * As such this test name is incorrect but this comment should clarify the history.
     */
    @Test
    public void testPartitionEdgeOnMultipleUserDefinedForeignKey() {
        LinkedHashMap<String, PropertyType> attributeMap = new LinkedHashMap<>();
        attributeMap.put("name", PropertyType.STRING);
        attributeMap.put("cmUid", PropertyType.STRING);
        attributeMap.put("vendorTechnology", PropertyType.STRING);

        VertexLabel realWorkspaceElementVertexLabel = sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "RealWorkspaceElement",
                attributeMap,
                ListOrderedSet.listOrderedSet(List.of("cmUid", "vendorTechnology")),
                PartitionType.LIST,
                "\"vendorTechnology\""
        );
        for (TEST test : TEST.values()) {
            Partition partition = realWorkspaceElementVertexLabel.ensureListPartitionExists(
                    test.name(),
                    "'" + test.name() + "'"
            );
        }
        PropertyColumn propertyColumn = realWorkspaceElementVertexLabel.getProperty("cmUid").orElseThrow(IllegalStateException::new);
        realWorkspaceElementVertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumn));

        VertexLabel virtualGroupVertexLabel = sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "VirtualGroup",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.STRING);
                    put("uid2", PropertyType.STRING);
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid1"))
        );

        EdgeLabel edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedEdgeLabelExistOnInOrOutVertexLabel(
                "virtualGroup_RealWorkspaceElement",
                virtualGroupVertexLabel,
                realWorkspaceElementVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid")),
                PartitionType.LIST,
                virtualGroupVertexLabel
        );


        this.sqlgGraph.tx().commit();

        Vertex v1 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "a", "vendorTechnology", TEST.TEST1.name());
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "b", "vendorTechnology", TEST.TEST2.name());
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "c", "vendorTechnology", TEST.TEST3.name());
        this.sqlgGraph.tx().commit();

        Vertex northern = this.sqlgGraph.addVertex(T.label, "VirtualGroup", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "Northern");
        Partition partition = edgeLabel.ensureListPartitionExists(
                "Northern",
                "'" + ((RecordId)northern.id()).getID().getIdentifiers().get(0).toString() + "'"
        );
        Vertex western = this.sqlgGraph.addVertex(T.label, "VirtualGroup", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "Western");
        partition = edgeLabel.ensureListPartitionExists(
                "Western",
                "'" + ((RecordId)western.id()).getID().getIdentifiers().get(0).toString() + "'"
        );
        Edge e = northern.addEdge("virtualGroup_RealWorkspaceElement", v1, "uid", UUID.randomUUID().toString());
        e.properties("what", "this");
        e = western.addEdge("virtualGroup_RealWorkspaceElement", v1, "uid", UUID.randomUUID().toString());
        e.properties("what", "this");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST1.name()).count().next(),0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST2.name()).count().next(),0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST3.name()).count().next(),0);
        Assert.assertEquals(3, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").count().next(),0);

        List<Vertex> vertices = this.sqlgGraph.traversal().V(v1).in("virtualGroup_RealWorkspaceElement").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(northern));
        Assert.assertTrue(vertices.contains(western));
    }

    @Test
    public void testPartitionEdge() {
        LinkedHashMap<String, PropertyType> attributeMap = new LinkedHashMap<>();
        attributeMap.put("name", PropertyType.STRING);
        attributeMap.put("cmUid", PropertyType.STRING);
        attributeMap.put("vendorTechnology", PropertyType.STRING);

        VertexLabel realWorkspaceElementVertexLabel = sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "RealWorkspaceElement",
                attributeMap,
                ListOrderedSet.listOrderedSet(List.of("cmUid", "vendorTechnology")),
                PartitionType.LIST,
                "\"vendorTechnology\""
        );
        for (TEST test : TEST.values()) {
            Partition partition = realWorkspaceElementVertexLabel.ensureListPartitionExists(
                    test.name(),
                    "'" + test.name() + "'"
            );
        }

        VertexLabel virtualGroupVertexLabel = sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "VirtualGroup",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.STRING);
                }}
        );

        EdgeLabel edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedEdgeLabelExist(
                "virtualGroup_RealWorkspaceElement",
                virtualGroupVertexLabel,
                realWorkspaceElementVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid", "name")),
                PartitionType.LIST,
                "name"
        );
        Partition partition = edgeLabel.ensureListPartitionExists(
                "Northern",
                "'Northern'"
        );
        partition = edgeLabel.ensureListPartitionExists(
                "Western",
                "'Western'"
        );


        this.sqlgGraph.tx().commit();
        String checkPrimaryKey = "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type\n" +
                "FROM   pg_index i\n" +
                "JOIN   pg_attribute a ON a.attrelid = i.indrelid\n" +
                "                     AND a.attnum = ANY(i.indkey)\n" +
                "WHERE  i.indrelid = '\"E_virtualGroup_RealWorkspaceElement\"'::regclass\n" +
                "AND    i.indisprimary;";
        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(checkPrimaryKey);
            List<Pair<String, String>> keys = new ArrayList<>();
            while (rs.next()) {
                String key = rs.getString("attname");
                String dataType = rs.getString("data_type");
                keys.add(Pair.of(key, dataType));
            }
            Assert.assertEquals(2, keys.size());
            Assert.assertEquals("uid", keys.get(0).getLeft());
            Assert.assertEquals("name", keys.get(1).getLeft());
        } catch (SQLException throwables) {
            Assert.fail(throwables.getMessage());
        }

        Vertex v1 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "a1", "vendorTechnology", TEST.TEST1.name());
        Vertex v11 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "a2", "vendorTechnology", TEST.TEST1.name());
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "b1", "vendorTechnology", TEST.TEST2.name());
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "c1", "vendorTechnology", TEST.TEST3.name());
        this.sqlgGraph.tx().commit();

        Vertex northern = this.sqlgGraph.addVertex(T.label, "VirtualGroup", "name", "Northern");
        Vertex western = this.sqlgGraph.addVertex(T.label, "VirtualGroup", "name", "Western");
        Edge e = northern.addEdge("virtualGroup_RealWorkspaceElement", v1, "name", "Northern", "uid", UUID.randomUUID().toString());
        e = northern.addEdge("virtualGroup_RealWorkspaceElement", v11, "name", "Northern", "uid", UUID.randomUUID().toString());
        e = western.addEdge("virtualGroup_RealWorkspaceElement", v1, "name", "Western", "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST1.name()).count().next(),0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST2.name()).count().next(),0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST3.name()).count().next(),0);
        Assert.assertEquals(4, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").count().next(),0);

        List<Vertex> vertices = this.sqlgGraph.traversal().V(v1).in("virtualGroup_RealWorkspaceElement").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertEquals(northern, vertices.get(0));
        Assert.assertEquals(western, vertices.get(1));
    }

    @Test
    public void testPartitionEdgeNoIdentifiers() {
        LinkedHashMap<String, PropertyType> attributeMap = new LinkedHashMap<>();
        attributeMap.put("name", PropertyType.STRING);
        attributeMap.put("cmUid", PropertyType.STRING);
        attributeMap.put("vendorTechnology", PropertyType.STRING);

        VertexLabel realWorkspaceElementVertexLabel = sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "RealWorkspaceElement",
                attributeMap,
                ListOrderedSet.listOrderedSet(List.of("cmUid", "vendorTechnology")),
                PartitionType.LIST,
                "\"vendorTechnology\""
        );
        for (TEST test : TEST.values()) {
            Partition partition = realWorkspaceElementVertexLabel.ensureListPartitionExists(
                    test.name(),
                    "'" + test.name() + "'"
            );
        }

        VertexLabel virtualGroupVertexLabel = sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "VirtualGroup",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.STRING);
                }}
        );

        EdgeLabel edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedEdgeLabelExist(
                "virtualGroup_RealWorkspaceElement",
                virtualGroupVertexLabel,
                realWorkspaceElementVertexLabel,
                new LinkedHashMap<>() {{
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of()),
                PartitionType.LIST,
                "name"
        );
        Partition partition = edgeLabel.ensureListPartitionExists(
                "Northern",
                "'Northern'"
        );
        partition = edgeLabel.ensureListPartitionExists(
                "Western",
                "'Western'"
        );


        this.sqlgGraph.tx().commit();

        Vertex v1 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "a1", "vendorTechnology", TEST.TEST1.name());
        Vertex v11 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "a2", "vendorTechnology", TEST.TEST1.name());
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "b1", "vendorTechnology", TEST.TEST2.name());
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "c1", "vendorTechnology", TEST.TEST3.name());
        this.sqlgGraph.tx().commit();

        Vertex northern = this.sqlgGraph.addVertex(T.label, "VirtualGroup", "name", "Northern");
        Vertex western = this.sqlgGraph.addVertex(T.label, "VirtualGroup", "name", "Western");
        Edge e = northern.addEdge("virtualGroup_RealWorkspaceElement", v1, "name", "Northern");
        e = northern.addEdge("virtualGroup_RealWorkspaceElement", v11, "name", "Northern");
        e = western.addEdge("virtualGroup_RealWorkspaceElement", v1, "name", "Western");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST1.name()).count().next(),0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST2.name()).count().next(),0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST3.name()).count().next(),0);
        Assert.assertEquals(4, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").count().next(),0);

        List<Vertex> vertices = this.sqlgGraph.traversal().V(v1).in("virtualGroup_RealWorkspaceElement").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertEquals(northern, vertices.get(0));
        Assert.assertEquals(western, vertices.get(1));

        //Check if the partitions are being used
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            boolean scanRealWorkspaceElementPartition = false;
            if (this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
                ResultSet rs = statement.executeQuery("explain SELECT\n" +
                        "\t\"public\".\"V_VirtualGroup\".\"ID\" AS \"alias1\",\n" +
                        "\t\"public\".\"V_VirtualGroup\".\"name\" AS \"alias2\"\n" +
                        "FROM\n" +
                        "\t\"public\".\"V_RealWorkspaceElement\" INNER JOIN\n" +
                        "\t\"public\".\"E_virtualGroup_RealWorkspaceElement\" ON \"public\".\"V_RealWorkspaceElement\".\"cmUid\" = \"public\".\"E_virtualGroup_RealWorkspaceElement\".\"public.RealWorkspaceElement.cmUid__I\" AND \"public\".\"V_RealWorkspaceElement\".\"vendorTechnology\" = \"public\".\"E_virtualGroup_RealWorkspaceElement\".\"public.RealWorkspaceElement.vendorTechnology__I\" INNER JOIN\n" +
                        "\t\"public\".\"V_VirtualGroup\" ON \"public\".\"E_virtualGroup_RealWorkspaceElement\".\"public.VirtualGroup__O\" = \"public\".\"V_VirtualGroup\".\"ID\"\n" +
                        "WHERE\n" +
                        "\t( \"public\".\"V_RealWorkspaceElement\".\"cmUid\" = 'a1' AND \"public\".\"V_RealWorkspaceElement\".\"vendorTechnology\" = 'TEST1')");
                while (rs.next()) {
                    String result = rs.getString(1);
                    scanRealWorkspaceElementPartition = scanRealWorkspaceElementPartition || result.contains("Index Only Scan using \"TEST1_pkey\"");
                }
            }
            Assert.assertTrue(scanRealWorkspaceElementPartition);
        } catch (SQLException ee) {
            Assert.fail(ee.getMessage());
        }

        vertices = this.sqlgGraph.traversal().V(v1).inE("virtualGroup_RealWorkspaceElement").has("name", "Northern").otherV().toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(northern, vertices.get(0));
        try (Statement statement = conn.createStatement()) {
            boolean scanRealWorkspaceElementPartition = false;
            boolean scanEdgePartition = false;
            if (this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
                ResultSet rs = statement.executeQuery("explain SELECT\n" +
                        "\t\"public\".\"V_RealWorkspaceElement\".\"cmUid\" AS \"alias1\",\n" +
                        "\t\"public\".\"V_RealWorkspaceElement\".\"vendorTechnology\" AS \"alias2\",\n" +
                        "\t\"public\".\"V_RealWorkspaceElement\".\"name\" AS \"alias3\",\n" +
                        "\t\"public\".\"E_virtualGroup_RealWorkspaceElement\".\"ID\" AS \"alias4\",\n" +
                        "\t\"public\".\"E_virtualGroup_RealWorkspaceElement\".\"name\" AS \"alias5\",\n" +
                        "\t\"public\".\"E_virtualGroup_RealWorkspaceElement\".\"public.VirtualGroup__O\" AS \"alias6\",\n" +
                        "\t\"public\".\"E_virtualGroup_RealWorkspaceElement\".\"public.RealWorkspaceElement.cmUid__I\" AS \"alias7\",\n" +
                        "\t\"public\".\"E_virtualGroup_RealWorkspaceElement\".\"public.RealWorkspaceElement.vendorTechnology__I\" AS \"alias8\",\n" +
                        "\t\"public\".\"V_VirtualGroup\".\"ID\" AS \"alias9\",\n" +
                        "\t\"public\".\"V_VirtualGroup\".\"name\" AS \"alias10\"\n" +
                        "FROM\n" +
                        "\t\"public\".\"V_RealWorkspaceElement\" INNER JOIN\n" +
                        "\t\"public\".\"E_virtualGroup_RealWorkspaceElement\" ON \"public\".\"V_RealWorkspaceElement\".\"cmUid\" = \"public\".\"E_virtualGroup_RealWorkspaceElement\".\"public.RealWorkspaceElement.cmUid__I\" AND \"public\".\"V_RealWorkspaceElement\".\"vendorTechnology\" = \"public\".\"E_virtualGroup_RealWorkspaceElement\".\"public.RealWorkspaceElement.vendorTechnology__I\" INNER JOIN\n" +
                        "\t\"public\".\"V_VirtualGroup\" ON \"public\".\"E_virtualGroup_RealWorkspaceElement\".\"public.VirtualGroup__O\" = \"public\".\"V_VirtualGroup\".\"ID\"\n" +
                        "WHERE\n" +
                        "\t( \"public\".\"V_RealWorkspaceElement\".\"cmUid\" = 'a1' AND \"public\".\"V_RealWorkspaceElement\".\"vendorTechnology\" = 'TEST1') AND ( \"public\".\"E_virtualGroup_RealWorkspaceElement\".\"name\" = 'Northern')\n");
                while (rs.next()) {
                    String result = rs.getString(1);
                    scanRealWorkspaceElementPartition = scanRealWorkspaceElementPartition || result.contains("Index Scan using \"TEST1_pkey\"");
                    scanEdgePartition = scanEdgePartition || result.contains("Index Scan using \"Northern_public.RealWorkspaceElement.cmUid__I_public.RealWo_idx\" on \"Northern\"");
                }
            }
            Assert.assertTrue(scanRealWorkspaceElementPartition);
            Assert.assertTrue(scanEdgePartition);
        } catch (SQLException ee) {
            Assert.fail(ee.getMessage());
        }
    }

    @Test
    public void testPartitionEdgeOnUserDefinedForeignKey() {
        LinkedHashMap<String, PropertyType> attributeMap = new LinkedHashMap<>();
        attributeMap.put("name", PropertyType.STRING);
        attributeMap.put("cmUid", PropertyType.STRING);
        attributeMap.put("vendorTechnology", PropertyType.STRING);

        VertexLabel realWorkspaceElementVertexLabel = sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "RealWorkspaceElement",
                attributeMap,
                ListOrderedSet.listOrderedSet(List.of("vendorTechnology", "cmUid")),
                PartitionType.LIST,
                "\"vendorTechnology\""
        );
        for (TEST test : TEST.values()) {
            Partition partition = realWorkspaceElementVertexLabel.ensureListPartitionExists(
                    test.name(),
                    "'" + test.name() + "'"
            );
        }

        VertexLabel virtualGroupVertexLabel = sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "VirtualGroup",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );

        EdgeLabel edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedEdgeLabelExistOnInOrOutVertexLabel(
                "virtualGroup_RealWorkspaceElement",
                virtualGroupVertexLabel,
                realWorkspaceElementVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid")),
                PartitionType.LIST,
                virtualGroupVertexLabel
        );


        this.sqlgGraph.tx().commit();

        Vertex v1 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "a", "vendorTechnology", TEST.TEST1.name());
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "b", "vendorTechnology", TEST.TEST2.name());
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "c", "vendorTechnology", TEST.TEST3.name());
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "d", "vendorTechnology", TEST.TEST1.name());
        this.sqlgGraph.tx().commit();

        Vertex northern = this.sqlgGraph.addVertex(T.label, "VirtualGroup", "uid", UUID.randomUUID().toString(), "name", "Northern");
        Partition partition = edgeLabel.ensureListPartitionExists(
                "Northern",
                "'" + ((RecordId)northern.id()).getID().getIdentifiers().get(0).toString() + "'"
        );
        Vertex western = this.sqlgGraph.addVertex(T.label, "VirtualGroup", "uid", UUID.randomUUID().toString(), "name", "Western");
        partition = edgeLabel.ensureListPartitionExists(
                "Western",
                "'" + ((RecordId)western.id()).getID().getIdentifiers().get(0).toString() + "'"
        );
        Edge edgeFromNorthernToV1 = northern.addEdge("virtualGroup_RealWorkspaceElement", v1, "uid", UUID.randomUUID().toString());
        Edge edgeFromWesternToV1 = western.addEdge("virtualGroup_RealWorkspaceElement", v1, "uid", UUID.randomUUID().toString());
        Edge edgeFromNorthernToV4 = northern.addEdge("virtualGroup_RealWorkspaceElement", v4, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST1.name()).count().next(),0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST2.name()).count().next(),0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST3.name()).count().next(),0);
        Assert.assertEquals(4, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").count().next(),0);

        List<Vertex> vertices = this.sqlgGraph.traversal().V(v4).in("virtualGroup_RealWorkspaceElement").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(northern));

        vertices = this.sqlgGraph.traversal().V(v1).in("virtualGroup_RealWorkspaceElement").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(northern));
        Assert.assertTrue(vertices.contains(western));
    }

    @Test
    public void testPartitionEdgeOnForeignKey() {
        LinkedHashMap<String, PropertyType> attributeMap = new LinkedHashMap<>();
        attributeMap.put("name", PropertyType.STRING);
        attributeMap.put("cmUid", PropertyType.STRING);
        attributeMap.put("vendorTechnology", PropertyType.STRING);

        VertexLabel realWorkspaceElementVertexLabel = sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "RealWorkspaceElement",
                attributeMap,
                ListOrderedSet.listOrderedSet(List.of("vendorTechnology", "cmUid")),
                PartitionType.LIST,
                "\"vendorTechnology\""
        );
        for (TEST test : TEST.values()) {
            Partition partition = realWorkspaceElementVertexLabel.ensureListPartitionExists(
                    test.name(),
                    "'" + test.name() + "'"
            );
        }
        PropertyColumn propertyColumn = realWorkspaceElementVertexLabel.getProperty("cmUid").orElseThrow(IllegalStateException::new);
        realWorkspaceElementVertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumn));

        VertexLabel virtualGroupVertexLabel = sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "VirtualGroup",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.STRING);
                }}
        );

        EdgeLabel edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedEdgeLabelExistOnInOrOutVertexLabel(
                "virtualGroup_RealWorkspaceElement",
                virtualGroupVertexLabel,
                realWorkspaceElementVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid")),
                PartitionType.LIST,
                virtualGroupVertexLabel
        );


        this.sqlgGraph.tx().commit();

        Vertex v1 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "a", "vendorTechnology", TEST.TEST1.name());
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "b", "vendorTechnology", TEST.TEST2.name());
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "c", "vendorTechnology", TEST.TEST3.name());
        this.sqlgGraph.tx().commit();

        Vertex northern = this.sqlgGraph.addVertex(T.label, "VirtualGroup", "name", "Northern");
        Partition partition = edgeLabel.ensureListPartitionExists(
                "Northern",
                ((RecordId) northern.id()).getID().getSequenceId().toString()
        );
        Vertex western = this.sqlgGraph.addVertex(T.label, "VirtualGroup", "name", "Western");
        partition = edgeLabel.ensureListPartitionExists(
                "Western",
                ((RecordId) western.id()).getID().getSequenceId().toString()
        );
        Edge e = northern.addEdge("virtualGroup_RealWorkspaceElement", v1, "uid", UUID.randomUUID().toString());
        e.properties("what", "this");
        e = western.addEdge("virtualGroup_RealWorkspaceElement", v1, "uid", UUID.randomUUID().toString());
        e.properties("what", "this");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST1.name()).count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST2.name()).count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST3.name()).count().next(), 0);
        Assert.assertEquals(3, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").count().next(), 0);

        List<Vertex> vertices = this.sqlgGraph.traversal().V(v1).in("virtualGroup_RealWorkspaceElement").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertEquals(northern, vertices.get(0));
        Assert.assertEquals(western, vertices.get(1));

        vertices = this.sqlgGraph.traversal().V(northern).out("virtualGroup_RealWorkspaceElement").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v1, vertices.get(0));
    }

    @Test
    public void testUpdatePropertiesOnEdgeToPartitionedTable() {
        LinkedHashMap<String, PropertyType> attributeMap = new LinkedHashMap<>();
        attributeMap.put("name", PropertyType.STRING);
        attributeMap.put("cmUid", PropertyType.STRING);
        attributeMap.put("vendorTechnology", PropertyType.STRING);

        VertexLabel realWorkspaceElementVertexLabel = sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "RealWorkspaceElement",
                attributeMap,
                ListOrderedSet.listOrderedSet(List.of("cmUid", "vendorTechnology")),
                PartitionType.LIST,
                "\"vendorTechnology\""
        );
        for (TEST test : TEST.values()) {
            Partition partition = realWorkspaceElementVertexLabel.ensureListPartitionExists(
                    test.name(),
                    "'" + test.name() + "'"
            );
        }
        PropertyColumn propertyColumn = realWorkspaceElementVertexLabel.getProperty("cmUid").orElseThrow(IllegalStateException::new);
        realWorkspaceElementVertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumn));
        this.sqlgGraph.tx().commit();

        Vertex v1 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "a", "vendorTechnology", TEST.TEST1.name());
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "b", "vendorTechnology", TEST.TEST2.name());
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "c", "vendorTechnology", TEST.TEST3.name());
        this.sqlgGraph.tx().commit();

        Vertex other = this.sqlgGraph.addVertex(T.label, "Other", "name", "other");
        Edge e = v1.addEdge("test", other);
        e.properties("what", "this");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST1.name()).count().next(),0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST2.name()).count().next(),0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST3.name()).count().next(),0);
        Assert.assertEquals(3, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").count().next(),0);

        List<Vertex> vertices = this.sqlgGraph.traversal().V(v1).out("test").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(other, vertices.get(0));


    }

    @Test
    public void testPartitionOnColumnWithCapitals() {
        LinkedHashMap<String, PropertyType> attributeMap = new LinkedHashMap<>();
        attributeMap.put("name", PropertyType.STRING);
        attributeMap.put("cmUid", PropertyType.STRING);
        attributeMap.put("vendorTechnology", PropertyType.STRING);

        VertexLabel realWorkspaceElementVertexLabel = sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "RealWorkspaceElement",
                attributeMap,
                ListOrderedSet.listOrderedSet(List.of("cmUid", "vendorTechnology")),
                PartitionType.LIST,
                "\"vendorTechnology\""
        );
        for (TEST test : TEST.values()) {
            Partition partition = realWorkspaceElementVertexLabel.ensureListPartitionExists(
                    test.name(),
                    "'" + test.name() + "'"
            );
        }
        PropertyColumn propertyColumn = realWorkspaceElementVertexLabel.getProperty("cmUid").orElseThrow(IllegalStateException::new);
        realWorkspaceElementVertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumn));
        sqlgGraph.tx().commit();
        sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "a", "vendorTechnology", TEST.TEST1.name());
        sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "b", "vendorTechnology", TEST.TEST2.name());
        sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "c", "vendorTechnology", TEST.TEST3.name());
        sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST1.name()).count().next(),0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST2.name()).count().next(),0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", TEST.TEST3.name()).count().next(),0);
        Assert.assertEquals(3, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").count().next(),0);

    }

    @Test
    public void testReloadVertexLabelWithNoPartitions() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("int1", "int2")),
                PartitionType.RANGE,
                "int1,int2");
        this.sqlgGraph.tx().commit();

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            VertexLabel a = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow();
            Assert.assertEquals("int1,int2", a.getPartitionExpression());
            Assert.assertEquals(PartitionType.RANGE, a.getPartitionType());
        }
    }

    @Test
    public void testReloadVertexLabelWithPartitions() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("int1", "int2")),
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
            a = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow();
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
    public void testReloadVertexLabelWithSubPartitions() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel a = publicSchema.ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("int1", "int2", "int3")),
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
            a = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow();
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
    public void testReloadEdgeLabelWithNoPartitions() {
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
                new LinkedHashMap<>() {{
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
                    .getEdgeLabel("ab").orElseThrow();
            Assert.assertEquals("int1,int2", edgeLabel.getPartitionExpression());
            Assert.assertEquals(PartitionType.RANGE, edgeLabel.getPartitionType());
        }
    }

    @Test
    public void testReloadEdgeLabelWithPartitions() {
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
                new LinkedHashMap<>() {{
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
                    .getEdgeLabel("ab").orElseThrow();
            Assert.assertEquals("int1", edgeLabel.getPartitionExpression());
            Assert.assertEquals(PartitionType.LIST, edgeLabel.getPartitionType());

            Assert.assertEquals(2, edgeLabel.getPartitions().size());
            Assert.assertTrue(edgeLabel.getPartitions().containsKey("int1"));
            Assert.assertTrue(edgeLabel.getPartitions().containsKey("int2"));

            p1 = edgeLabel.getPartition("int1").orElseThrow();
            Assert.assertEquals(PartitionType.NONE, p1.getPartitionType());
            Assert.assertEquals("1, 2, 3, 4, 5", p1.getIn());
            p2 = edgeLabel.getPartition("int2").orElseThrow();
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
                new LinkedHashMap<>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("int1", "int2", "int3")),
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
                    .getEdgeLabel("ab").orElseThrow();
            Assert.assertEquals("int1", edgeLabel.getPartitionExpression());
            Assert.assertEquals(PartitionType.LIST, edgeLabel.getPartitionType());

            Assert.assertEquals(2, edgeLabel.getPartitions().size());
            Assert.assertTrue(edgeLabel.getPartitions().containsKey("p11"));
            Assert.assertTrue(edgeLabel.getPartitions().containsKey("p12"));

            p1 = edgeLabel.getPartition("p11").orElseThrow();
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


            p2 = edgeLabel.getPartition("p12").orElseThrow();
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
            Partition p1211 = p121.getPartition("p1211").orElseThrow();
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
                new LinkedHashMap<>() {{
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

        Partition partition = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Measurement").orElseThrow().getPartition("measurement1").orElseThrow();
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
                new LinkedHashMap<>() {{
                    put("name", PropertyType.STRING);
                    put("population", PropertyType.LONG);
                }},
                ListOrderedSet.listOrderedSet(List.of("name")),
                PartitionType.LIST,
                "left(lower(name), 1)",
                false);
        partitionedVertexLabel.ensureListPartitionExists("Cities_a", "'a'");
        partitionedVertexLabel.ensureListPartitionExists("Cities_b", "'b'");
        partitionedVertexLabel.ensureListPartitionExists("Cities_c", "'c'");
        partitionedVertexLabel.ensureListPartitionExists("Cities_d", "'d'");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "Cities", "name", "aasbc", "population", 1000L, "uid", i);
        }
        this.sqlgGraph.addVertex(T.label, "Cities", "name", "basbc", "population", 1000L, "uid", 1);
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "Cities", "name", "casbc", "population", 1000L, "uid", i);
        }
        this.sqlgGraph.addVertex(T.label, "Cities", "name", "dasbc", "population", 1000L, "uid", 1);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(202, this.sqlgGraph.traversal().V().hasLabel("Cities").count().next(), 0);
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("Cities").has("name", "aasbc").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Cities").has("name", "basbc").count().next(), 0);
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("Cities").has("name", "casbc").count().next(), 0);

        Partition partition = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Cities").orElseThrow().getPartition("Cities_a").orElseThrow();
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
                new LinkedHashMap<>() {{
                    put("name", PropertyType.STRING);
                    put("uid", PropertyType.STRING);
                    put("population", PropertyType.LONG);
                }},
                ListOrderedSet.listOrderedSet(List.of("name", "uid")),
                PartitionType.LIST,
                "name");
        partitionedVertexLabel.ensureListPartitionExists("Cities_a", "'London'");
        partitionedVertexLabel.ensureListPartitionExists("Cities_b", "'New York'");
        partitionedVertexLabel.ensureListPartitionExists("Cities_c", "'Paris'");
        partitionedVertexLabel.ensureListPartitionExists("Cities_d", "'Johannesburg'");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "Cities", "name", "London", "population", 1000L, "uid", "uid_London_" + i);
        }
        this.sqlgGraph.addVertex(T.label, "Cities", "name", "New York", "population", 1000L, "uid", "uid_NewYork_1");
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "Cities", "name", "Paris", "population", 1000L, "uid", "uid_Paris_" + i);
        }
        this.sqlgGraph.addVertex(T.label, "Cities", "name", "Johannesburg", "population", 1000L, "uid", "uid_Johannesburg_1");
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
                new LinkedHashMap<>() {{
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

        Partition partition = this.sqlgGraph.getTopology().getSchema("test").orElseThrow().getVertexLabel("Measurement").orElseThrow().getPartition("measurement1").orElseThrow();
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
                new LinkedHashMap<>() {{
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
                    sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("liveAt").orElseThrow().getPartitionExpression()
            );
            Assert.assertEquals(
                    PartitionType.RANGE,
                    sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("liveAt").orElseThrow().getPartitionType()
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
                new LinkedHashMap<>() {{
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
                new LinkedHashMap<>() {{
                    put("name", PropertyType.STRING);
                    put("logdate", PropertyType.LOCALDATE);
                    put("peaktemp", PropertyType.INTEGER);
                    put("unitsales", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name", "logdate", "peaktemp")),
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
                new LinkedHashMap<>() {{
                    put("name", PropertyType.STRING);
                    put("list1", PropertyType.STRING);
                    put("list2", PropertyType.INTEGER);
                    put("unitsales", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("name", "list1", "list2")),
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
                new LinkedHashMap<>() {{
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("int1", "int2")),
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
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid", "int1", "int2")),
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
