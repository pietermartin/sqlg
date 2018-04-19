package org.umlg.sqlg.test.sharding;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/04/03
 */
public class TestSharding extends BaseTest {

    @Before
    public void before() throws Exception {
        super.before();
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsSharding());
    }

    @Test
    public void testSharding1() throws Exception {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist(
                "A",
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        PropertyColumn dist = aVertexLabel.getProperty("dist").orElseThrow(() -> new RuntimeException("BUG"));
        aVertexLabel.ensureDistributed(8, dist);
        PropertyColumn distributionPropertyColumn = this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("A").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(8, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, aVertexLabel));
        Assert.assertNull(this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("A").get().getDistributionColocate());
        Assert.assertEquals(8, this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("A").get().getShardCount());

        VertexLabel bVertexLabel = aSchema.ensureVertexLabelExist(
                "B",
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        dist = bVertexLabel.getProperty("dist").orElseThrow(() -> new RuntimeException("BUG"));
        bVertexLabel.ensureDistributed(8, dist, aVertexLabel);
        distributionPropertyColumn = this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("B").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(8, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("B").get().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("B").get().getDistributionColocate());
        Assert.assertEquals(8, this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("B").get().getShardCount());

        this.sqlgGraph.tx().commit();

        Assert.assertEquals(8, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, aVertexLabel));
        distributionPropertyColumn = this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("A").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals("dist", distributionPropertyColumn.getName());
        Assert.assertNull(this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("A").get().getDistributionColocate());
        Assert.assertEquals(8, this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("A").get().getShardCount());

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
        Assert.assertTrue(this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("A").get().isDistributed());
        Assert.assertEquals(8, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, aVertexLabel));
        distributionPropertyColumn = this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("A").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals("dist", distributionPropertyColumn.getName());
        Assert.assertNull(this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("A").get().getDistributionColocate());
        Assert.assertEquals(8, this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("A").get().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("B").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(8, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("B").get().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("B").get().getDistributionColocate());
        Assert.assertEquals(8, this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("B").get().getShardCount());
    }

    @Test
    public void testSharding2() throws Exception {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        aVertexLabel.ensureDistributed(4, aVertexLabel.getProperty("dist").orElseThrow(IllegalStateException::new));
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new HashMap<String, PropertyType>() {{
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
                new HashMap<String, PropertyType>() {{
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

        PropertyColumn distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, aVertexLabel));
        Assert.assertNull(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getShardCount());

        this.sqlgGraph.tx().commit();

        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, aVertexLabel));
        Assert.assertNull(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getShardCount());
    }

    @Test
    public void testShardingDifferentInAndOut() throws Exception {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        aVertexLabel.ensureDistributed(4, aVertexLabel.getProperty("dist").orElseThrow(IllegalStateException::new));
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new HashMap<String, PropertyType>() {{
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
                new HashMap<String, PropertyType>() {{
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

        PropertyColumn distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, aVertexLabel));
        Assert.assertNull(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getShardCount());

        this.sqlgGraph.tx().commit();

        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, aVertexLabel));
        Assert.assertNull(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getShardCount());

        distributionPropertyColumn = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getDistributionPropertyColumn();
        Assert.assertNotNull(distributionPropertyColumn);
        Assert.assertEquals(4, this.sqlgGraph.getSqlDialect().getShardCount(this.sqlgGraph, bVertexLabel));
        Assert.assertNotNull(this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getDistributionColocate());
        Assert.assertEquals(aVertexLabel, this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getDistributionColocate());
        Assert.assertEquals(4, this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getShardCount());
    }

    @Test
    public void testShardingWithPartition() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "A",
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("date", PropertyType.LOCALDATE);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist")),
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

}
