package org.umlg.sqlg.test.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.Partition;
import org.umlg.sqlg.structure.topology.PartitionType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2021/10/11
 */
@SuppressWarnings("DuplicatedCode")
public class TestHashPartitioning extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestHashPartitioning.class);

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
    public void testHashPartition() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.INTEGER);
                    put("uid2", PropertyType.LONG);
                    put("uid3", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid1", "uid2", "uid3")),
                PartitionType.HASH,
                "\"uid1\""
        );
        for (int i = 0; i < 10; i++) {
            vertexLabel.ensureHashPartitionExists("hashPartition" + i, 10, i);
        }
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 1000; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "uid1", i, "uid2", 1L, "uid3", "halo1");
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1000, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement s = connection.createStatement()) {
            ResultSet rs = s.executeQuery("select tableoid::regclass as partition_name, count(*) from \"V_A\" group by 1 order by 1;");
            int count = 0;
            Map<String, Long> partitionDistributionCount = new HashMap<>();
            while (rs.next()) {
                count++;
                partitionDistributionCount.put(rs.getString(1), rs.getLong(2));
            }
            Assert.assertEquals(10, count);
            Assert.assertEquals(10, partitionDistributionCount.size());
            for (int i = 0; i < 10; i++) {
                Assert.assertTrue(partitionDistributionCount.containsKey("\"hashPartition" + i + "\""));
            }
            Assert.assertEquals(100, partitionDistributionCount.get("\"hashPartition0\""), 0);
            Assert.assertEquals(92, partitionDistributionCount.get("\"hashPartition1\""), 0);
            Assert.assertEquals(103, partitionDistributionCount.get("\"hashPartition2\""), 0);
            Assert.assertEquals(88, partitionDistributionCount.get("\"hashPartition3\""), 0);
            Assert.assertEquals(113, partitionDistributionCount.get("\"hashPartition4\""), 0);
            Assert.assertEquals(90, partitionDistributionCount.get("\"hashPartition5\""), 0);
            Assert.assertEquals(119, partitionDistributionCount.get("\"hashPartition6\""), 0);
            Assert.assertEquals(92, partitionDistributionCount.get("\"hashPartition7\""), 0);
            Assert.assertEquals(100, partitionDistributionCount.get("\"hashPartition8\""), 0);
            Assert.assertEquals(103, partitionDistributionCount.get("\"hashPartition9\""), 0);
        } catch (SQLException throwables) {
            Assert.fail(throwables.getMessage());
        }

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            VertexLabel a = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow();
            Assert.assertEquals("\"uid1\"", a.getPartitionExpression());
            Assert.assertEquals(PartitionType.HASH, a.getPartitionType());

            for (int i = 0; i < 10; i++) {
                Optional<Partition> part1 = a.getPartition("hashPartition" + i);
                Assert.assertTrue(part1.isPresent());
                Assert.assertNull(part1.get().getPartitionExpression());
                Assert.assertEquals(10, part1.get().getModulus(), 0);
                Assert.assertEquals(i, part1.get().getRemainder(), 0);
            }
        }

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            VertexLabel a = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow();
            //This is actually wrong, the quotes have been dropped.
            Assert.assertEquals("uid1", a.getPartitionExpression());
            Assert.assertEquals(PartitionType.HASH, a.getPartitionType());

            for (int i = 0; i < 10; i++) {
                Optional<Partition> part1 = a.getPartition("hashPartition" + i);
                Assert.assertTrue(part1.isPresent());
                Assert.assertNull(part1.get().getPartitionExpression());
                Assert.assertEquals(10, part1.get().getModulus(), 0);
                Assert.assertEquals(i, part1.get().getRemainder(), 0);
            }
        }

    }

    @Test
    public void testHashPartitionOnEdge() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.INTEGER);
                    put("uid2", PropertyType.LONG);
                    put("uid3", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid1", "uid2", "uid3")),
                PartitionType.HASH,
                "\"uid1\""
        );
        for (int i = 0; i < 10; i++) {
            aVertexLabel.ensureHashPartitionExists("aHashPartition" + i, 10, i);
        }
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.INTEGER);
                    put("uid2", PropertyType.LONG);
                    put("uid3", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid1", "uid2", "uid3")),
                PartitionType.HASH,
                "\"uid1\""
        );
        for (int i = 0; i < 10; i++) {
            bVertexLabel.ensureHashPartitionExists("bHashPartition" + i, 10, i);
        }
        EdgeLabel edgeLabel = aVertexLabel.ensurePartitionedEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.INTEGER);
                    put("uid2", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid1", "uid2")),
                PartitionType.HASH,
                "\"uid1\""
        );
        for (int i = 0; i < 10; i++) {
            edgeLabel.ensureHashPartitionExists("eHashPartition" + i, 10, i);
        }
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 1000; i++) {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A", "uid1", i, "uid2", 1L, "uid3", "halo1");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B", "uid1", i, "uid2", 1L, "uid3", "halo1");
            a.addEdge("ab", b, "uid1", i, "uid2", "uid2" + i);
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1000, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
        Assert.assertEquals(1000, this.sqlgGraph.traversal().V().hasLabel("B").count().next(), 0);
        Assert.assertEquals(1000, this.sqlgGraph.traversal().E().hasLabel("ab").count().next(), 0);
        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement s = connection.createStatement()) {
            ResultSet rs = s.executeQuery("select tableoid::regclass as partition_name, count(*) from \"E_ab\" group by 1 order by 1;");
            int count = 0;
            Map<String, Long> partitionDistributionCount = new HashMap<>();
            while (rs.next()) {
                count++;
                partitionDistributionCount.put(rs.getString(1), rs.getLong(2));
            }
            Assert.assertEquals(10, count);
            Assert.assertEquals(10, partitionDistributionCount.size());
            for (int i = 0; i < 10; i++) {
                Assert.assertTrue(partitionDistributionCount.containsKey("\"eHashPartition" + i + "\""));
            }
            Assert.assertEquals(100, partitionDistributionCount.get("\"eHashPartition0\""), 0);
            Assert.assertEquals(92, partitionDistributionCount.get("\"eHashPartition1\""), 0);
            Assert.assertEquals(103, partitionDistributionCount.get("\"eHashPartition2\""), 0);
            Assert.assertEquals(88, partitionDistributionCount.get("\"eHashPartition3\""), 0);
            Assert.assertEquals(113, partitionDistributionCount.get("\"eHashPartition4\""), 0);
            Assert.assertEquals(90, partitionDistributionCount.get("\"eHashPartition5\""), 0);
            Assert.assertEquals(119, partitionDistributionCount.get("\"eHashPartition6\""), 0);
            Assert.assertEquals(92, partitionDistributionCount.get("\"eHashPartition7\""), 0);
            Assert.assertEquals(100, partitionDistributionCount.get("\"eHashPartition8\""), 0);
            Assert.assertEquals(103, partitionDistributionCount.get("\"eHashPartition9\""), 0);
        } catch (SQLException throwables) {
            Assert.fail(throwables.getMessage());
        }

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            EdgeLabel a = sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
            Assert.assertEquals("\"uid1\"", a.getPartitionExpression());
            Assert.assertEquals(PartitionType.HASH, a.getPartitionType());

            for (int i = 0; i < 10; i++) {
                Optional<Partition> part1 = a.getPartition("eHashPartition" + i);
                Assert.assertTrue(part1.isPresent());
                Assert.assertNull(part1.get().getPartitionExpression());
                Assert.assertEquals(10, part1.get().getModulus(), 0);
                Assert.assertEquals(i, part1.get().getRemainder(), 0);
            }
        }

        //Delete the topology
        dropSqlgSchema(this.sqlgGraph);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            VertexLabel a = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow();
            //This is actually wrong, the quotes have been dropped.
            Assert.assertEquals("uid1", a.getPartitionExpression());
            Assert.assertEquals(PartitionType.HASH, a.getPartitionType());

            for (int i = 0; i < 10; i++) {
                Optional<Partition> part1 = a.getPartition("aHashPartition" + i);
                Assert.assertTrue(part1.isPresent());
                Assert.assertNull(part1.get().getPartitionExpression());
                Assert.assertEquals(10, part1.get().getModulus(), 0);
                Assert.assertEquals(i, part1.get().getRemainder(), 0);
            }
            VertexLabel b = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow();
            //This is actually wrong, the quotes have been dropped.
            Assert.assertEquals("uid1", b.getPartitionExpression());
            Assert.assertEquals(PartitionType.HASH, b.getPartitionType());

            for (int i = 0; i < 10; i++) {
                Optional<Partition> part1 = b.getPartition("bHashPartition" + i);
                Assert.assertTrue(part1.isPresent());
                Assert.assertNull(part1.get().getPartitionExpression());
                Assert.assertEquals(10, part1.get().getModulus(), 0);
                Assert.assertEquals(i, part1.get().getRemainder(), 0);
            }

            EdgeLabel ab = sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
            //Reloading from information schema drops the quotes.
            Assert.assertEquals("uid1", ab.getPartitionExpression());
            Assert.assertEquals(PartitionType.HASH, ab.getPartitionType());

            for (int i = 0; i < 10; i++) {
                Optional<Partition> part1 = ab.getPartition("eHashPartition" + i);
                Assert.assertTrue(part1.isPresent());
                Assert.assertNull(part1.get().getPartitionExpression());
                Assert.assertEquals(10, part1.get().getModulus(), 0);
                Assert.assertEquals(i, part1.get().getRemainder(), 0);
            }
        }

    }

    @Test
    public void testHashPartitionCopyCommand() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.INTEGER);
                    put("uid2", PropertyType.LONG);
                    put("uid3", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid1", "uid2", "uid3")),
                PartitionType.HASH,
                "\"uid1\""
        );
        for (int i = 0; i < 10; i++) {
            vertexLabel.ensureHashPartitionExists("hashPartition" + i, 10, i);
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 1000; i++) {
            this.sqlgGraph.streamVertex(T.label, "A", "uid1", i, "uid2", 1L, "uid3", "halo1");
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1000, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement s = connection.createStatement()) {
            ResultSet rs = s.executeQuery("select tableoid::regclass as partition_name, count(*) from \"V_A\" group by 1 order by 1;");
            int count = 0;
            Map<String, Long> partitionDistributionCount = new HashMap<>();
            while (rs.next()) {
                count++;
                partitionDistributionCount.put(rs.getString(1), rs.getLong(2));
            }
            Assert.assertEquals(10, count);
            Assert.assertEquals(10, partitionDistributionCount.size());
            for (int i = 0; i < 10; i++) {
                Assert.assertTrue(partitionDistributionCount.containsKey("\"hashPartition" + i + "\""));
            }
            Assert.assertEquals(100, partitionDistributionCount.get("\"hashPartition0\""), 0);
            Assert.assertEquals(92, partitionDistributionCount.get("\"hashPartition1\""), 0);
            Assert.assertEquals(103, partitionDistributionCount.get("\"hashPartition2\""), 0);
            Assert.assertEquals(88, partitionDistributionCount.get("\"hashPartition3\""), 0);
            Assert.assertEquals(113, partitionDistributionCount.get("\"hashPartition4\""), 0);
            Assert.assertEquals(90, partitionDistributionCount.get("\"hashPartition5\""), 0);
            Assert.assertEquals(119, partitionDistributionCount.get("\"hashPartition6\""), 0);
            Assert.assertEquals(92, partitionDistributionCount.get("\"hashPartition7\""), 0);
            Assert.assertEquals(100, partitionDistributionCount.get("\"hashPartition8\""), 0);
            Assert.assertEquals(103, partitionDistributionCount.get("\"hashPartition9\""), 0);
        } catch (SQLException throwables) {
            Assert.fail(throwables.getMessage());
        }
    }

    @Test
    public void testHashSubPartition() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.INTEGER);
                    put("uid2", PropertyType.LONG);
                    put("uid3", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid1", "uid2", "uid3")),
                PartitionType.HASH,
                "\"uid1\""
        );
        for (int i = 0; i < 10; i++) {
            Partition partition = vertexLabel.ensureHashPartitionWithSubPartitionExists("p" + i, 10, i, PartitionType.HASH, "uid2");
            for (int j = 0; j < 10; j++) {
                partition.ensureHashPartitionExists("p" + i + "_" + j, 10, j);
            }
        }
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 1000; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "uid1", i, "uid2", 1L, "uid3", "halo1");
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1000, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);

    }

    @Test
    public void testHashPartitionMultipleGraphs() throws InterruptedException {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            VertexLabel vertexLabel = sqlgGraph1.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                    "A",
                    new LinkedHashMap<>() {{
                        put("uid1", PropertyType.INTEGER);
                        put("uid2", PropertyType.LONG);
                        put("uid3", PropertyType.STRING);
                    }},
                    ListOrderedSet.listOrderedSet(List.of("uid1", "uid2", "uid3")),
                    PartitionType.HASH,
                    "\"uid1\""
            );
            for (int i = 0; i < 10; i++) {
                Partition partition = vertexLabel.ensureHashPartitionWithSubPartitionExists("p" + i, 10, i, PartitionType.HASH, "uid2");
                for (int j = 0; j < 10; j++) {
                    partition.ensureHashPartitionExists("p" + i + "_" + j, 10, j);
                }
            }
            sqlgGraph1.tx().commit();
            Thread.sleep(1000);

            LOGGER.info(sqlgGraph1.getTopology().toString());
            LOGGER.info(this.sqlgGraph.getTopology().toString());
            Assert.assertEquals(sqlgGraph1.getTopology(), this.sqlgGraph.getTopology());
        }

    }

    @Test
    public void testHashSubPartitionMultipleGraphs() throws InterruptedException {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            VertexLabel vertexLabel = sqlgGraph1.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
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
            for (int i = 0; i < 10; i++) {
                Partition partition = vertexLabel.ensureListPartitionWithSubPartitionExists("p" + i, String.valueOf(i), PartitionType.HASH, "uid2");
                for (int j = 0; j < 10; j++) {
                    partition.ensureHashPartitionExists("p" + i + "_" + j, 10, j);
                }
            }
            sqlgGraph1.tx().commit();
            Thread.sleep(1000);

            LOGGER.info(sqlgGraph1.getTopology().toString());
            LOGGER.info(this.sqlgGraph.getTopology().toString());
            Assert.assertEquals(sqlgGraph1.getTopology(), this.sqlgGraph.getTopology());
        }

    }

    @Test
    public void testHashSubPartition2MultipleGraphs() throws InterruptedException {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            VertexLabel vertexLabel = sqlgGraph1.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                    "A",
                    new LinkedHashMap<>() {{
                        put("uid1", PropertyType.INTEGER);
                        put("uid2", PropertyType.LONG);
                        put("uid3", PropertyType.STRING);
                    }},
                    ListOrderedSet.listOrderedSet(List.of("uid1", "uid2", "uid3")),
                    PartitionType.HASH,
                    "\"uid1\""
            );
            for (int i = 0; i < 10; i++) {
                Partition partition = vertexLabel.ensureHashPartitionWithSubPartitionExists("p" + i, 10, i, PartitionType.LIST, "uid2");
                for (int j = 0; j < 10; j++) {
                    partition.ensureListPartitionExists("p" + i + "_" + j, String.valueOf(j));
                }
            }
            sqlgGraph1.tx().commit();
            Thread.sleep(1000);

            LOGGER.info(sqlgGraph1.getTopology().toString());
            LOGGER.info(this.sqlgGraph.getTopology().toString());
            Assert.assertEquals(sqlgGraph1.getTopology(), this.sqlgGraph.getTopology());
        }

    }
}
