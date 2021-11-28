package org.umlg.sqlg.test.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.*;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.TopologyChangeAction;
import org.umlg.sqlg.structure.TopologyInf;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/01/21
 */
@SuppressWarnings("unused")
public class TestPartitionMultipleGraphs extends BaseTest {

    private final List<Triple<TopologyInf, TopologyInf, TopologyChangeAction>> topologyListenerTriple = new ArrayList<>();

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
        this.topologyListenerTriple.clear();
    }

    @Test
    public void testNotifyPartitionedVertexLabel() {
        TestTopologyChangeListener.TopologyListenerTest tlt = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(tlt);
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {

            Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
            VertexLabel measurement = publicSchema.ensurePartitionedVertexLabelExist(
                    "Measurement",
                    new HashMap<>() {{
                        put("uid", PropertyType.STRING);
                        put("date", PropertyType.LOCALDATE);
                        put("temp", PropertyType.INTEGER);
                    }},
                    ListOrderedSet.listOrderedSet(List.of("uid", "date")),
                    PartitionType.RANGE,
                    "date");
            Partition p1 = measurement.ensureRangePartitionExists("m1", "'2016-07-01'", "'2016-08-01'");
            Partition p2 = measurement.ensureRangePartitionExists("m2", "'2016-08-01'", "'2016-09-01'");

            Assert.assertTrue(tlt.receivedEvent(measurement, TopologyChangeAction.CREATE));
            Assert.assertTrue(tlt.receivedEvent(p1, TopologyChangeAction.CREATE));

            this.sqlgGraph.tx().commit();

            Thread.sleep(2000);

            Optional<VertexLabel> measurementAgain = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("Measurement");
            Assert.assertTrue(measurementAgain.isPresent());
            Assert.assertEquals(PartitionType.RANGE, measurementAgain.get().getPartitionType());
            Assert.assertEquals("date", measurementAgain.get().getPartitionExpression());
            Optional<Partition> m1 = measurementAgain.get().getPartition("m1");
            Assert.assertTrue(m1.isPresent());
            Assert.assertEquals("'2016-07-01'", m1.get().getFrom());
            Assert.assertEquals("'2016-08-01'", m1.get().getTo());
            Optional<Partition> m2 = measurementAgain.get().getPartition("m2");
            Assert.assertTrue(m2.isPresent());
            Assert.assertEquals("'2016-08-01'", m2.get().getFrom());
            Assert.assertEquals("'2016-09-01'", m2.get().getTo());

            //Drop a partition, check that the drop is propagated.
            p1.remove(false);
            Assert.assertTrue(tlt.receivedEvent(p1, TopologyChangeAction.DELETE));
            this.sqlgGraph.tx().commit();
            Assert.assertEquals(1, measurement.getPartitions().size(), 0);
            Thread.sleep(2000);

            Assert.assertEquals(1, sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("Measurement").orElseThrow().getPartitions().size(), 0);

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNotifyPartitionedEdgeLabel() {
        TestTopologyChangeListener.TopologyListenerTest tlt = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(tlt);
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {

            Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
            VertexLabel person = publicSchema.ensureVertexLabelExist("Person");
            VertexLabel address = publicSchema.ensureVertexLabelExist("Address");
            EdgeLabel livesAt = person.ensurePartitionedEdgeLabelExist(
                    "livesAt",
                    address,
                    new HashMap<>() {{
                        put("uid", PropertyType.STRING);
                        put("date", PropertyType.LOCALDATE);
                    }},
                    ListOrderedSet.listOrderedSet(List.of("uid", "date")),
                    PartitionType.RANGE,
                    "date");
            Partition p1 = livesAt.ensureRangePartitionExists("m1", "'2016-07-01'", "'2016-08-01'");
            Partition p2 = livesAt.ensureRangePartitionExists("m2", "'2016-08-01'", "'2016-09-01'");
            Assert.assertTrue(tlt.receivedEvent(livesAt, TopologyChangeAction.CREATE));
            Assert.assertTrue(tlt.receivedEvent(p1, TopologyChangeAction.CREATE));
            Assert.assertTrue(tlt.receivedEvent(p2, TopologyChangeAction.CREATE));
            this.sqlgGraph.tx().commit();
            Thread.sleep(2000);

            publicSchema = sqlgGraph1.getTopology().getPublicSchema();
            Optional<EdgeLabel> livesAtOther = publicSchema.getEdgeLabel("livesAt");
            Assert.assertTrue(livesAtOther.isPresent());
            Assert.assertEquals(PartitionType.RANGE, livesAtOther.get().getPartitionType());
            Assert.assertEquals("date", livesAtOther.get().getPartitionExpression());

            Optional<Partition> m1 = livesAtOther.get().getPartition("m1");
            Assert.assertTrue(m1.isPresent());
            Assert.assertEquals("'2016-07-01'", m1.get().getFrom());
            Assert.assertEquals("'2016-08-01'", m1.get().getTo());
            Optional<Partition> m2 = livesAtOther.get().getPartition("m2");
            Assert.assertTrue(m2.isPresent());
            Assert.assertEquals("'2016-08-01'", m2.get().getFrom());
            Assert.assertEquals("'2016-09-01'", m2.get().getTo());

            p1.remove(false);
            this.sqlgGraph.tx().commit();
            Assert.assertTrue(tlt.receivedEvent(p1, TopologyChangeAction.DELETE));

            Thread.sleep(2000);
            publicSchema = sqlgGraph1.getTopology().getPublicSchema();
            Assert.assertEquals(1, publicSchema.getEdgeLabel("livesAt").orElseThrow().getPartitions().size(), 0);

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNotificationSubSubPartitions() throws InterruptedException {
        TestTopologyChangeListener.TopologyListenerTest tlt = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(tlt);
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel a = aSchema.ensurePartitionedVertexLabelExist(
                "A",
                new HashMap<>(){{
                    put("uid", PropertyType.STRING);
                    put("int1", PropertyType.INTEGER);
                    put("int2", PropertyType.INTEGER);
                    put("int3", PropertyType.INTEGER);
                    put("int4", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid", "int1", "int2", "int3", "int4")),
                PartitionType.LIST,
                "int1"
        );
        Partition p1 = a.ensureListPartitionWithSubPartitionExists("int1", "1,2,3,4,5", PartitionType.LIST, "int2");
        Partition p2 = p1.ensureListPartitionWithSubPartitionExists("int2", "1,2,3,4,5", PartitionType.LIST, "int3");
        Partition p3 = p2.ensureListPartitionWithSubPartitionExists("int3", "1,2,3,4,5", PartitionType.LIST, "int4");
        p3.ensureListPartitionExists("int4", "1,2,3,4,5");
        this.sqlgGraph.tx().commit();
        Thread.sleep(1000);

        a = this.sqlgGraph1.getTopology().getSchema("A").orElseThrow().getVertexLabel("A").orElseThrow();
        Optional<Partition> p = a.getPartition("int4");
        Assert.assertTrue(p.isPresent());
        Assert.assertNull(p.get().getFrom());
        Assert.assertNull(p.get().getTo());
        Assert.assertEquals("1,2,3,4,5", p.get().getIn());

        Assert.assertTrue(tlt.receivedEvent(p1, TopologyChangeAction.CREATE));
        Assert.assertTrue(tlt.receivedEvent(p2, TopologyChangeAction.CREATE));
        Assert.assertTrue(tlt.receivedEvent(p3, TopologyChangeAction.CREATE));

        p3.remove();
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(tlt.receivedEvent(p3, TopologyChangeAction.DELETE));
        p2.remove();
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(tlt.receivedEvent(p2, TopologyChangeAction.DELETE));
        p1.remove();
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(tlt.receivedEvent(p1, TopologyChangeAction.DELETE));
        Thread.sleep(1000);
        a = this.sqlgGraph1.getTopology().getSchema("A").orElseThrow().getVertexLabel("A").orElseThrow();
        p = a.getPartition("int4");
        Assert.assertFalse(p.isPresent());
        p = a.getPartition("int3");
        Assert.assertFalse(p.isPresent());
        p = a.getPartition("int2");
        Assert.assertFalse(p.isPresent());
        p = a.getPartition("int1");
        Assert.assertFalse(p.isPresent());
    }

}
