package org.umlg.sqlg.test.batch;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

public class TestBatchServerSideEdgeCreation extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testBulkEdges() {
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
        this.sqlgGraph.bulkAddEdges("A", "B", "AB", Pair.of("index", "index"), uids);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("B").count().next(), 0);
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("A").out().count().next(), 0);
    }

    @Test
    public void testBulkEdgesCrossSchemas() {
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

        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("A.A").count().next(), 0);
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("B.B").count().next(), 0);
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("A.A").out().count().next(), 0);
    }

    @Test
    public void testBulkEdges2() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().streamingBatchModeOn();
        List<Pair<String, String>> uids = new ArrayList<>();
        LinkedHashMap properties = new LinkedHashMap();
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
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();

        this.sqlgGraph.tx().streamingBatchModeOn();
        SchemaTable person = SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "Person");
        this.sqlgGraph.bulkAddEdges("Person", "Person", "friend", Pair.of("id", "id"), uids);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());

        GraphTraversal<Vertex, Vertex> has = this.sqlgGraph.traversal().V().hasLabel("Person").has("id", uuid1Cache);
        Assert.assertTrue(has.hasNext());
        Vertex person50 = has.next();

        GraphTraversal<Vertex, Vertex> has1 = this.sqlgGraph.traversal().V().hasLabel("Person").has("id", uuid2Cache);
        Assert.assertTrue(has1.hasNext());
        Vertex person250 = has1.next();
        Assert.assertTrue(this.sqlgGraph.traversal().V(person50.id()).out().hasNext());
        Vertex person250Please = this.sqlgGraph.traversal().V(person50.id()).out().next();
        Assert.assertEquals(person250, person250Please);
    }

    @Test
    public void testBulkEdgesTempTableUnique() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        List<Pair<String, String>> uids = new ArrayList<>();
        LinkedHashMap properties = new LinkedHashMap();
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
        SchemaTable person = SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "Person");
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
        person = SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "Person");
        this.sqlgGraph.bulkAddEdges("Person", "Person", "friend", Pair.of("id", "id"), uids);
        this.sqlgGraph.tx().commit();

    }

}
