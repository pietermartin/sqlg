package org.umlg.sqlg.test.sharding;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/04/03
 */
@SuppressWarnings("DuplicatedCode")
public class TestSharding extends BaseTest {

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
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsSharding());
    }

    @Test
    public void testShardCount1() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist(
                "A",
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        PropertyColumn dist = aVertexLabel.getProperty("dist").orElseThrow(() -> new RuntimeException("BUG"));
        aVertexLabel.ensureDistributed(32, dist);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A.A", "uid", UUID.randomUUID().toString(), "dist", "a", "value", "1");
        this.sqlgGraph.addVertex(T.label, "A.A", "uid", UUID.randomUUID().toString(), "dist", "b", "value", "2");
        this.sqlgGraph.addVertex(T.label, "A.A", "uid", UUID.randomUUID().toString(), "dist", "c", "value", "3");
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testShardingVertex() throws Exception {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist(
                "A",
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        PropertyColumn dist = aVertexLabel.getProperty("dist").orElseThrow(() -> new RuntimeException("BUG"));
        aVertexLabel.ensureDistributed(8, dist);
        PropertyColumn distributionPropertyColumn = this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("A")).orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(8, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, aVertexLabel));
        Assert.assertNull(this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("A")).orElseThrow().getDistributionColocate());
        Assert.assertEquals(8, this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("A")).orElseThrow().getShardCount());

        VertexLabel bVertexLabel = aSchema.ensureVertexLabelExist(
                "B",
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        dist = bVertexLabel.getProperty("dist").orElseThrow(() -> new RuntimeException("BUG"));
        bVertexLabel.ensureDistributed(8, dist, aVertexLabel);

        distributionPropertyColumn = this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("B")).orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(8, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("B")).orElseThrow().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("B")).orElseThrow().getDistributionColocate());
        Assert.assertEquals(8, this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("B")).orElseThrow().getShardCount());

        this.sqlgGraph.tx().commit();

        //check meta data is on sqlgGraph1
        Thread.sleep(1000);

        Assert.assertEquals(8, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, aVertexLabel));
        distributionPropertyColumn = this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("A")).orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals("dist", distributionPropertyColumn.getName());
        Assert.assertNull(this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("A")).orElseThrow().getDistributionColocate());
        Assert.assertEquals(8, this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("A")).orElseThrow().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("B")).orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals("dist", distributionPropertyColumn.getName());
        Assert.assertNotNull(this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("B")).orElseThrow().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("B")).orElseThrow().getDistributionColocate());
        Assert.assertEquals(8, this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("A")).orElseThrow().getShardCount());

        Assert.assertEquals(8, this.sqlgGraph1.getSqlDialect().getShardCount(this.sqlgGraph1, aVertexLabel));
        distributionPropertyColumn = this.sqlgGraph1.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("A")).orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals("dist", distributionPropertyColumn.getName());
        Assert.assertNull(this.sqlgGraph1.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("A")).orElseThrow().getDistributionColocate());
        Assert.assertEquals(8, this.sqlgGraph1.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("A")).orElseThrow().getShardCount());

        distributionPropertyColumn = this.sqlgGraph1.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("B")).orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals("dist", distributionPropertyColumn.getName());
        Assert.assertNotNull(this.sqlgGraph1.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("B")).orElseThrow().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph1.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("B")).orElseThrow().getDistributionColocate());
        Assert.assertEquals(8, this.sqlgGraph1.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("A")).orElseThrow().getShardCount());

        char[] alphabet = "abcd".toCharArray();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 1000; i++) {
            int j = i % 4;
            char x = alphabet[j];
            this.sqlgGraph.streamVertex(T.label, "A.A", "uid", UUID.randomUUID().toString(), "dist", Character.toString(x), "value", Integer.toString(i));
        }
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(250, this.sqlgGraph.traversal().V().hasLabel("A.A").has("dist", "a").toList().size());

        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        Assert.assertTrue(this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("A")).orElseThrow().isDistributed());
        Assert.assertEquals(8, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, aVertexLabel));
        distributionPropertyColumn = this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("A")).orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals("dist", distributionPropertyColumn.getName());
        Assert.assertNull(this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("A")).orElseThrow().getDistributionColocate());
        Assert.assertEquals(8, this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("A")).orElseThrow().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("B")).orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(8, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("B")).orElseThrow().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("B")).orElseThrow().getDistributionColocate());
        Assert.assertEquals(8, this.sqlgGraph.getTopology().getSchema("A").flatMap(a -> a.getVertexLabel("B")).orElseThrow().getShardCount());
    }

    @Test
    public void testShardingEdge() throws Exception {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        aVertexLabel.ensureDistributed(4, aVertexLabel.getProperty("dist").orElseThrow(IllegalStateException::new));
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        bVertexLabel.ensureDistributed(4, bVertexLabel.getProperty("dist").orElseThrow(IllegalStateException::new), aVertexLabel);

        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        edgeLabel.ensureDistributed(4, edgeLabel.getProperty("dist").orElseThrow(RuntimeException::new));

        List<String> tenantIds = Arrays.asList("RNC1", "RNC2", "RNC3", "RNC4");
        int i = 1;
        String tenantId = tenantIds.get(i);
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "dist", tenantId, "value", Integer.toString(i));
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "dist", tenantId, "value", Integer.toString(i));
        Edge e = a.addEdge("ab", b, "uid", UUID.randomUUID().toString(), "dist", tenantId, "value", Integer.toString(i));
        this.sqlgGraph.tx().commit();

        Thread.sleep(1000);

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").toList().size());
        Assert.assertEquals(a, this.sqlgGraph.traversal().V().hasLabel("A").toList().get(0));
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("B").toList().size());
        Assert.assertEquals(b, this.sqlgGraph.traversal().V().hasLabel("B").toList().get(0));
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("ab").toList().size());
        Assert.assertEquals(e, this.sqlgGraph.traversal().E().hasLabel("ab").toList().get(0));

        Assert.assertEquals(1, this.sqlgGraph1.traversal().V().hasLabel("A").toList().size());
        Assert.assertEquals(a, this.sqlgGraph1.traversal().V().hasLabel("A").toList().get(0));
        Assert.assertEquals(1, this.sqlgGraph1.traversal().V().hasLabel("B").toList().size());
        Assert.assertEquals(b, this.sqlgGraph1.traversal().V().hasLabel("B").toList().get(0));
        Assert.assertEquals(1, this.sqlgGraph1.traversal().E().hasLabel("ab").toList().size());
        Assert.assertEquals(e, this.sqlgGraph1.traversal().E().hasLabel("ab").toList().get(0));

        PropertyColumn distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, aVertexLabel));
        Assert.assertNull(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getShardCount());

        distributionPropertyColumn = this.sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph1.getSqlDialect().getShardCount(this.sqlgGraph, aVertexLabel));
        Assert.assertNull(this.sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getShardCount());

        distributionPropertyColumn = this.sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph1.getSqlDialect().getShardCount(this.sqlgGraph1, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getShardCount());

        distributionPropertyColumn = this.sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph1.getSqlDialect().getShardCount(this.sqlgGraph1, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getShardCount());

        this.sqlgGraph.tx().commit();

        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, aVertexLabel));
        Assert.assertNull(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getShardCount());

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").toList().size());
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("B").toList().size());
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("ab").toList().size());
    }

    @Test
    public void testShardingDifferentInAndOut() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        aVertexLabel.ensureDistributed(4, aVertexLabel.getProperty("dist").orElseThrow(IllegalStateException::new));
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        bVertexLabel.ensureDistributed(4, bVertexLabel.getProperty("dist").orElseThrow(IllegalStateException::new), aVertexLabel);

        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        edgeLabel.ensureDistributed(4, edgeLabel.getProperty("dist").orElseThrow(RuntimeException::new));

        List<String> tenantIds = Arrays.asList("RNC1", "RNC2", "RNC3", "RNC4");
        int i = 1;
        String tenantId = tenantIds.get(i);
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "dist", tenantId, "value", Integer.toString(i));
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "dist", tenantId, "value", Integer.toString(i));
        Edge e = a.addEdge("ab", b, "uid", UUID.randomUUID().toString(), "dist", tenantId, "value", Integer.toString(i));
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").toList().size());
        Assert.assertEquals(a, this.sqlgGraph.traversal().V().hasLabel("A").toList().get(0));
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("B").toList().size());
        Assert.assertEquals(b, this.sqlgGraph.traversal().V().hasLabel("B").toList().get(0));
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("ab").toList().size());
        Assert.assertEquals(e, this.sqlgGraph.traversal().E().hasLabel("ab").toList().get(0));

        PropertyColumn distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, aVertexLabel));
        Assert.assertNull(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getShardCount());

        this.sqlgGraph.tx().commit();

        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, aVertexLabel));
        Assert.assertNull(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getShardCount());
    }

    @Test
    public void testShardingWithPartition() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "A",
                new HashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("date", PropertyType.LOCALDATE);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "date", "dist")),
                PartitionType.RANGE,
                "date"
        );
        aVertexLabel.ensureRangePartitionExists("july", "'2016-07-01'", "'2016-08-01'");
        aVertexLabel.ensureRangePartitionExists("august", "'2016-08-01'", "'2016-09-01'");
        PropertyColumn dist = aVertexLabel.getProperty("dist").orElseThrow(IllegalStateException::new);
        aVertexLabel.ensureDistributed(32, dist);
        this.sqlgGraph.tx().commit();

        LocalDate localDate1 = LocalDate.of(2016, 7, 1);
        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "dist", "a", "date", localDate1, "value", "1");
        LocalDate localDate2 = LocalDate.of(2016, 8, 1);
        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "dist", "b", "date", localDate2, "value", "1");
        this.sqlgGraph.tx().commit();

    }

//    @Test
    public void test() {
        List<LocalDateTime> times = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            LocalDateTime locaDateTime = LocalDateTime.now().minus(i, ChronoUnit.SECONDS);
            times.add(locaDateTime);
            this.sqlgGraph.addVertex(T.label, "A", "dateTime", locaDateTime);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("dateTime", P.within(times)).toList();
        Assert.assertEquals(1000, vertices.size());
    }

//    @Test
    public void testShardRNC() {
        VertexLabel rncVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "ObjectType1",
                new HashMap<>() {{
                    put("rncId", PropertyType.STRING);
                    put("cellId", PropertyType.STRING);
                    put("dateTime", PropertyType.LOCALDATETIME);
                    put("date", PropertyType.LOCALDATE);
                    put("count1", PropertyType.LONG);
                    put("count2", PropertyType.LONG);
                    put("count3", PropertyType.LONG);
                    put("count4", PropertyType.LONG);
                    put("count5", PropertyType.LONG);
                    put("count6", PropertyType.LONG);
                    put("count7", PropertyType.LONG);
                    put("count8", PropertyType.LONG);
                    put("count9", PropertyType.LONG);
                    put("count10", PropertyType.LONG);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("rncId", "cellId")),
                PartitionType.RANGE,
                "date"
        );
        rncVertexLabel.ensureRangePartitionExists("day1", "'2018-05-03'", "'2018-05-04'");
        rncVertexLabel.ensureRangePartitionExists("day2", "'2018-05-04'", "'2018-05-05'");
        rncVertexLabel.ensureRangePartitionExists("day3", "'2018-05-05'", "'2018-05-06'");
        rncVertexLabel.ensureRangePartitionExists("day4", "'2018-05-06'", "'2018-05-07'");
        rncVertexLabel.ensureRangePartitionExists("day5", "'2018-05-07'", "'2018-05-08'");
        rncVertexLabel.ensureRangePartitionExists("day6", "'2018-05-08'", "'2018-05-09'");
        rncVertexLabel.ensureRangePartitionExists("day7", "'2018-05-09'", "'2018-05-10'");
        rncVertexLabel.ensureRangePartitionExists("day8", "'2018-05-10'", "'2018-05-11'");
        rncVertexLabel.ensureRangePartitionExists("day9", "'2018-05-11'", "'2018-05-12'");
        rncVertexLabel.ensureRangePartitionExists("day10", "'2018-05-12'", "'2018-05-13'");
        rncVertexLabel.ensureRangePartitionExists("day11", "'2018-05-13'", "'2018-05-14'");
        rncVertexLabel.ensureRangePartitionExists("day12", "'2018-05-14'", "'2018-05-15'");
        rncVertexLabel.ensureRangePartitionExists("day13", "'2018-05-15'", "'2018-05-16'");
        rncVertexLabel.ensureRangePartitionExists("day14", "'2018-05-16'", "'2018-05-17'");

        rncVertexLabel.ensureDistributed(32, rncVertexLabel.getProperty("rncId").get());
        this.sqlgGraph.tx().commit();

        StopWatch stopWatch = StopWatch.createStarted();
        List<String> rncs = Arrays.asList("RNC1", "RNC2", "RNC3", "RNC4", "RNC5", "RNC6", "RNC7", "RNC8", "RNC9", "RNC10");
        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<Future<Boolean>> futures = new ArrayList<>();
        LocalDateTime start = LocalDateTime.of(2018, 5, 4, 0, 0);
        for (String rnc : rncs) {
            futures.add(executor.submit(() -> {
                sqlgGraph.tx().normalBatchModeOn();
                for (long i = 1; i <= 1_000_000; i++) {
                    LocalDateTime next = start.plusSeconds(i);
                    sqlgGraph.addVertex(T.label, "ObjectType1",
                            "rncId", rnc,
                            "cellId", "cellId_" + i,
                            "dateTime", next,
                            "date", next.toLocalDate(),
                            "count1", i,
                            "count2", i,
                            "count3", i,
                            "count4", i,
                            "count5", i,
                            "count6", i,
                            "count7", i,
                            "count8", i,
                            "count9", i,
                            "count10", i
                    );
                    if (i % 10_000 == 0) {
                        sqlgGraph.tx().flush();
                    }
                }
                sqlgGraph.tx().commit();
                return true;
            }));

        }
        executor.shutdown();
        for (Future<Boolean> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail(e.getMessage());
            }
        }
        stopWatch.stop();
        System.out.println(stopWatch);
    }

}
