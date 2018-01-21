package org.umlg.sqlg.test.topology;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.*;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.TopologyChangeAction;
import org.umlg.sqlg.structure.TopologyInf;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;
import org.umlg.sqlg.test.topology.TestTopologyChangeListener.TopologyListenerTest;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/01/21
 */
public class TestPartitionMultipleGraphs extends BaseTest {

    private List<Triple<TopologyInf, String, TopologyChangeAction>> topologyListenerTriple = new ArrayList<>();

    @SuppressWarnings("Duplicates")
    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
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
        this.topologyListenerTriple.clear();
    }

    @Test
    public void testNotifyPartitionedVertexLabel() {
        TopologyListenerTest tlt = new TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(tlt);
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {

            Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
            VertexLabel measurement = publicSchema.ensurePartitionedVertexLabelExist(
                    "Measurement",
                    new HashMap<String, PropertyType>() {{
                        put("date", PropertyType.LOCALDATE);
                        put("temp", PropertyType.INTEGER);
                    }},
                    PartitionType.RANGE,
                    "date");
            Partition p1 = measurement.ensurePartitionExists("m1", "'2016-07-01'", "'2016-08-01'");
            Partition p2 = measurement.ensurePartitionExists("m2", "'2016-08-01'", "'2016-09-01'");

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

            Assert.assertEquals(1, sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("Measurement").get().getPartitions().size(), 0);


        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNotifyPartitionedEdgeLabel() {
        TopologyListenerTest tlt = new TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(tlt);
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {

            Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
            VertexLabel person = publicSchema.ensureVertexLabelExist("Person");
            VertexLabel address = publicSchema.ensureVertexLabelExist("Address");
            EdgeLabel livesAt = person.ensurePartitionedEdgeLabelExist(
                    "livesAt",
                    address,
                    new HashMap<String, PropertyType>() {{
                        put("date", PropertyType.LOCALDATE);
                    }},
                    PartitionType.RANGE,
                    "date");
            Partition p1 = livesAt.ensurePartitionExists("m1", "'2016-07-01'", "'2016-08-01'");
            Partition p2 = livesAt.ensurePartitionExists("m2", "'2016-08-01'", "'2016-09-01'");
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
            Assert.assertEquals(1, publicSchema.getEdgeLabel("livesAt").get().getPartitions().size(), 0);

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}
