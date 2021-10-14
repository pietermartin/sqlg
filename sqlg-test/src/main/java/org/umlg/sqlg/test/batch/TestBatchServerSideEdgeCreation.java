package org.umlg.sqlg.test.batch;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.PartitionType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("Duplicates")
public class TestBatchServerSideEdgeCreation extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestBatchServerSideEdgeCreation.class);

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStreamingBatchMode());
    }

    private void testBulkEdges_assert(SqlgGraph sqlgGraph) { assertEquals(10, sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
        assertEquals(100, sqlgGraph.traversal().V().hasLabel("B").count().next(), 0);
        assertEquals(100, sqlgGraph.traversal().V().hasLabel("A").out().count().next(), 0);
        assertEquals("y", sqlgGraph.traversal().E().hasLabel("AB").values("x").dedup().next());
    }

    @Test
    public void testBulkEdgesOnUserSuppliedIds() throws InterruptedException {

        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                this.sqlgGraph.getSqlDialect().getPublicSchema(),
                "A",
                new LinkedHashMap<>() {{
                    put("index", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("index"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                this.sqlgGraph.getSqlDialect().getPublicSchema(),
                "B",
                new LinkedHashMap<>() {{
                    put("index", PropertyType.INTEGER);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("index"))
        );
        EdgeLabel edgeLabel = sqlgGraph.getTopology().getPublicSchema().ensurePartitionedEdgeLabelExist(
                "AB",
                aVertexLabel,
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("part", PropertyType.STRING);
                    put("x", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of()),
                PartitionType.LIST,
                "part"
        );
        edgeLabel.ensureListPartitionExists("part1", "'part1'");
        edgeLabel.ensureListPartitionExists("part2", "'part2'");

        this.sqlgGraph.tx().normalBatchModeOn();
        int count = 0;
        List<Pair<String, String>> uids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "index", Integer.toString(i));
            for (int j = 0; j < 10; j++) {
                this.sqlgGraph.addVertex(T.label, "B", "index", Integer.toString(count));
                uids.add(Pair.of(Integer.toString(i), Integer.toString(count++)));
            }
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.bulkAddEdges(
                "A",
                "B",
                "AB",
                Pair.of("index", "index"),
                uids,
                "part", "part1",
                "x", "y");
        this.sqlgGraph.tx().commit();

        testBulkEdges_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBulkEdges_assert(this.sqlgGraph1);
        }
    }

    @Test
    public void testBulkEdges() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        int count = 0;
        List<Pair<String, String>> uids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "index", Integer.toString(i));
            for (int j = 0; j < 10; j++) {
                this.sqlgGraph.addVertex(T.label, "B", "index", Integer.toString(count));
                uids.add(Pair.of(Integer.toString(i), Integer.toString(count++)));
            }
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.bulkAddEdges("A", "B", "AB", Pair.of("index", "index"), uids, "x", "y");
        this.sqlgGraph.tx().commit();

        testBulkEdges_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBulkEdges_assert(this.sqlgGraph1);
        }
    }


    @Test
    public void testBulkEdgesCrossSchemas() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        int count = 0;
        List<Pair<String, String>> uids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "A.A", "index", Integer.toString(i));
            for (int j = 0; j < 10; j++) {
                this.sqlgGraph.addVertex(T.label, "B.B", "index", Integer.toString(count));
                uids.add(Pair.of(Integer.toString(i), Integer.toString(count++)));
            }
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.bulkAddEdges("A.A", "B.B", "AB", Pair.of("index", "index"), uids);
        this.sqlgGraph.tx().commit();

        testBulkEdgesCrossSchemas_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBulkEdgesCrossSchemas_assert(this.sqlgGraph1);
        }
    }

    private void testBulkEdgesCrossSchemas_assert(SqlgGraph sqlgGraph) {
        assertEquals(10, sqlgGraph.traversal().V().hasLabel("A.A").count().next(), 0);
        assertEquals(100, sqlgGraph.traversal().V().hasLabel("B.B").count().next(), 0);
        assertEquals(100, sqlgGraph.traversal().V().hasLabel("A.A").out().count().next(), 0);
    }

    @Test
    public void testBulkEdges2() throws InterruptedException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().streamingBatchModeOn();
        List<Pair<String, String>> uids = new ArrayList<>();
        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        String uuid1Cache = null;
        String uuid2Cache = null;
        for (int i = 0; i < 1000; i++) {
            String uuid1 = UUID.randomUUID().toString();
            String uuid2 = UUID.randomUUID().toString();
            if (i == 50) {
                uuid1Cache = uuid1;
                uuid2Cache = uuid2;
            }
            uids.add(Pair.of(uuid1, uuid2));
            properties.put("id", uuid1);
            this.sqlgGraph.streamVertex("Person", properties);
            properties.put("id", uuid2);
            this.sqlgGraph.streamVertex("Person", properties);
        }
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.debug(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();

        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.bulkAddEdges("Person", "Person", "friend", Pair.of("id", "id"), uids);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.debug(stopWatch.toString());

        testBulkEdges2_assert(this.sqlgGraph, uuid1Cache, uuid2Cache);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBulkEdges2_assert(this.sqlgGraph1, uuid1Cache, uuid2Cache);
        }
    }

    private void testBulkEdges2_assert(SqlgGraph sqlgGraph, String uuid1Cache, String uuid2Cache) {
        GraphTraversal<Vertex, Vertex> has = sqlgGraph.traversal().V().hasLabel("Person").has("id", uuid1Cache);
        Assert.assertTrue(has.hasNext());
        Vertex person50 = has.next();

        GraphTraversal<Vertex, Vertex> has1 = sqlgGraph.traversal().V().hasLabel("Person").has("id", uuid2Cache);
        Assert.assertTrue(has1.hasNext());
        Vertex person250 = has1.next();
        Assert.assertTrue(sqlgGraph.traversal().V(person50.id()).out().hasNext());
        Vertex person250Please = sqlgGraph.traversal().V(person50.id()).out().next();
        assertEquals(person250, person250Please);
    }

    @Test
    public void testBulkEdgesTempTableUnique() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        List<Pair<String, String>> uids = new ArrayList<>();
        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        for (int i = 0; i < 1000; i++) {
            String uuid1 = UUID.randomUUID().toString();
            String uuid2 = UUID.randomUUID().toString();
            uids.add(Pair.of(uuid1, uuid2));
            properties.put("id", uuid1);
            this.sqlgGraph.streamVertex("Person", properties);
            properties.put("id", uuid2);
            this.sqlgGraph.streamVertex("Person", properties);
        }
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.bulkAddEdges("Person", "Person", "friend", Pair.of("id", "id"), uids);
        this.sqlgGraph.tx().commit();

        //and again
        this.sqlgGraph.tx().streamingBatchModeOn();
        uids.clear();
        for (int i = 0; i < 1000; i++) {
            String uuid1 = UUID.randomUUID().toString();
            String uuid2 = UUID.randomUUID().toString();
            uids.add(Pair.of(uuid1, uuid2));
            properties.put("id", uuid1);
            this.sqlgGraph.streamVertex("Person", properties);
            properties.put("id", uuid2);
            this.sqlgGraph.streamVertex("Person", properties);
        }
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.bulkAddEdges("Person", "Person", "friend", Pair.of("id", "id"), uids);
        this.sqlgGraph.tx().commit();

    }

    @Test
    public void testBulkAddEdgesStringAndIntegerIds() throws InterruptedException {
        Vertex realWorkspaceElement1 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "a");
        Vertex realWorkspaceElement2 = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "b");
        Vertex virtualGroup = this.sqlgGraph.addVertex(T.label, "VirtualGroup", "name", "asd");
        this.sqlgGraph.tx().commit();
        Edge e =realWorkspaceElement1.addEdge("realWorkspaceElement_virtualGroup", virtualGroup);
        this.sqlgGraph.tx().commit();
        e.remove();
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().streamingBatchModeOn();
        List<Pair<String, Integer>> ids = new ArrayList<>();
        ids.add(Pair.of("a", 1));
        ids.add(Pair.of("b", 1));
        this.sqlgGraph.bulkAddEdges("RealWorkspaceElement", "VirtualGroup", "realWorkspaceElement_virtualGroup", Pair.of("cmUid", "ID"), ids);
        this.sqlgGraph.tx().commit();

        testBulkAddEdgeStringAndIntegerIds_assert(this.sqlgGraph, realWorkspaceElement1, realWorkspaceElement2, virtualGroup);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBulkAddEdgeStringAndIntegerIds_assert(this.sqlgGraph1, realWorkspaceElement1, realWorkspaceElement2, virtualGroup);
        }
    }

    private void testBulkAddEdgeStringAndIntegerIds_assert(SqlgGraph sqlgGraph, Vertex realWorkspaceElement1, Vertex realWorkspaceElement2, Vertex virtualGroup) {
        Assert.assertTrue(sqlgGraph.traversal().V(realWorkspaceElement1.id()).out("realWorkspaceElement_virtualGroup").hasNext());
        Assert.assertTrue(sqlgGraph.traversal().V(realWorkspaceElement2.id()).out("realWorkspaceElement_virtualGroup").hasNext());
        Assert.assertTrue(sqlgGraph.traversal().V(virtualGroup.id()).in("realWorkspaceElement_virtualGroup").hasNext());
    }

    @Test
    public void testOnIds() throws InterruptedException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Vertex virtualGroup = sqlgGraph.addVertex(T.label, "VirtualGroup", "name", "halo");
        sqlgGraph.tx().commit();
        List<Pair<Long, Long>> ids = new ArrayList<>();
        sqlgGraph.tx().streamingWithLockBatchModeOn();
        Map<String, Object> properties = new HashMap<>();
        properties.put("cm_uid", UUID.randomUUID().toString());
        for (int i = 0; i < 1_000; i++) {
            Vertex v = sqlgGraph.addVertex("RealWorkspaceElement", properties);
            RecordId recordId = (RecordId) v.id();
            Pair<Long, Long> idPair = Pair.of(recordId.sequenceId(), ((RecordId) virtualGroup.id()).sequenceId());
            ids.add(idPair);
        }
        sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.debug("Time to insert: " + stopWatch);
        stopWatch.reset();
        stopWatch.start();
        this.sqlgGraph.tx().streamingBatchModeOn();
        sqlgGraph.bulkAddEdges("RealWorkspaceElement", "VirtualGroup", "realWorkspaceElement_virtualGroup", Pair.of("ID", "ID"), ids);
        sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.debug("Time to insert: " + stopWatch);
        assertEquals(1_000, this.sqlgGraph.traversal().V(virtualGroup).in("realWorkspaceElement_virtualGroup").count().next().intValue());
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            assertEquals(1_000, this.sqlgGraph1.traversal().V(virtualGroup).in("realWorkspaceElement_virtualGroup").count().next().intValue());
        }
    }

    @Test
    public void testBulkAddEdgesINinStringOut() throws InterruptedException {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "lala", "a1");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "B", "cmuid", "a2");
        this.sqlgGraph.tx().commit();
        List<Pair<Long, String>> ids = new ArrayList<>();
        Pair<Long, String> idCmUidPair = Pair.of(((RecordId)v1.id()).sequenceId(), "a2");
        ids.add(idCmUidPair);

        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.bulkAddEdges("A", "B", "ab", Pair.of("ID", "cmuid"), ids);
        this.sqlgGraph.tx().commit();

        testBulkAddEdgesINinStringOut_assert(this.sqlgGraph, v1, v2);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBulkAddEdgesINinStringOut_assert(this.sqlgGraph1, v1, v2);
        }
    }

    private void testBulkAddEdgesINinStringOut_assert(SqlgGraph sqlgGraph, Vertex v1, Vertex v2) {
        assertEquals(1, sqlgGraph.traversal().V(v1).out("ab").count().next().intValue());
        assertEquals(v2.id(), sqlgGraph.traversal().V(v1).out("ab").next().id());

    }

}
