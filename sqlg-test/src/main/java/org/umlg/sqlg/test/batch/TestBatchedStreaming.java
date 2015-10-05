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
import org.umlg.sqlg.structure.BatchCallback;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Date: 2015/10/03
 * Time: 8:53 PM
 */
public class TestBatchedStreaming extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testStreamingWithBatchSize() {
        final int BATCH_SIZE = 1000;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        LinkedHashMap properties = new LinkedHashMap();
        this.sqlgGraph.tx().streamingBatchMode(BATCH_SIZE);
        List<Pair<SqlgVertex, SqlgVertex>> uids = new ArrayList<>();
        String uuidCache1 = null;
        String uuidCache2 = null;
        for (int i = 1; i <= 1000; i++) {
            String uuid1 = UUID.randomUUID().toString();
            String uuid2 = UUID.randomUUID().toString();
            if (i == 50) {
                uuidCache1 = uuid1;
                uuidCache2 = uuid2;
            }
            properties.put("id", uuid1);
            SqlgVertex v1 = this.sqlgGraph.streamVertexFixedBatch("Person", properties);
            properties.put("id", uuid2);
            SqlgVertex v2 = this.sqlgGraph.streamVertexFixedBatch("Person", properties);
            uids.add(Pair.of(v1, v2));
            if (i % (BATCH_SIZE / 2) == 0) {
                for (Pair<SqlgVertex, SqlgVertex> uid : uids) {
                    uid.getLeft().streamFixedBatchEdge("friend", uid.getRight());
                }
                //This is needed because the number of edges are less than the batch size so it will not be auto flushed
                this.sqlgGraph.tx().flush();
                uids.clear();
                this.sqlgGraph.tx().streamingBatchMode(BATCH_SIZE);
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();

        Assert.assertEquals(2000, this.sqlgGraph.traversal().V().hasLabel("Person").count().next(), 0);
        Assert.assertEquals(1000, this.sqlgGraph.traversal().E().hasLabel("friend").count().next(), 0);

        GraphTraversal<Vertex, Vertex> has = this.sqlgGraph.traversal().V().hasLabel("Person").has("id", uuidCache1);
        Assert.assertTrue(has.hasNext());
        Vertex person50 = has.next();

        GraphTraversal<Vertex, Vertex> has1 = this.sqlgGraph.traversal().V().hasLabel("Person").has("id", uuidCache2);
        Assert.assertTrue(has1.hasNext());
        Vertex person250 = has1.next();
        Assert.assertTrue(this.sqlgGraph.traversal().V(person50.id()).out().hasNext());
        Vertex person250Please = this.sqlgGraph.traversal().V(person50.id()).out().next();
        Assert.assertEquals(person250, person250Please);
    }

    @Test
    public void testStreamingWithBatchSizeNonDefaultSchema() {
        final int BATCH_SIZE = 1000;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        LinkedHashMap properties = new LinkedHashMap();
        this.sqlgGraph.tx().streamingBatchMode(BATCH_SIZE);
        List<Pair<SqlgVertex, SqlgVertex>> uids = new ArrayList<>();
        String uuidCache1 = null;
        String uuidCache2 = null;
        for (int i = 1; i <= 1000; i++) {
            String uuid1 = UUID.randomUUID().toString();
            String uuid2 = UUID.randomUUID().toString();
            if (i == 50) {
                uuidCache1 = uuid1;
                uuidCache2 = uuid2;
            }
            properties.put("id", uuid1);
            SqlgVertex v1 = this.sqlgGraph.streamVertexFixedBatch("A.Person", properties);
            properties.put("id", uuid2);
            SqlgVertex v2 = this.sqlgGraph.streamVertexFixedBatch("A.Person", properties);
            uids.add(Pair.of(v1, v2));
            if (i % (BATCH_SIZE / 2) == 0) {
                for (Pair<SqlgVertex, SqlgVertex> uid : uids) {
                    uid.getLeft().streamFixedBatchEdge("friend", uid.getRight());
                }
                //This is needed because the number of edges are less than the batch size so it will not be auto flushed
                this.sqlgGraph.tx().flush();
                uids.clear();
                this.sqlgGraph.tx().streamingBatchMode(BATCH_SIZE);
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();

        Assert.assertEquals(2000, this.sqlgGraph.traversal().V().hasLabel("A.Person").count().next(), 0);
        Assert.assertEquals(1000, this.sqlgGraph.traversal().E().hasLabel("A.friend").count().next(), 0);

        GraphTraversal<Vertex, Vertex> has = this.sqlgGraph.traversal().V().hasLabel("A.Person").has("id", uuidCache1);
        Assert.assertTrue(has.hasNext());
        Vertex person50 = has.next();

        GraphTraversal<Vertex, Vertex> has1 = this.sqlgGraph.traversal().V().hasLabel("A.Person").has("id", uuidCache2);
        Assert.assertTrue(has1.hasNext());
        Vertex person250 = has1.next();
        Assert.assertTrue(this.sqlgGraph.traversal().V(person50.id()).out().hasNext());
        Vertex person250Please = this.sqlgGraph.traversal().V(person50.id()).out().next();
        Assert.assertEquals(person250, person250Please);
    }

    @Test
    public void testStreamingWithBatchSizeWithCallBack() {
        final int BATCH_SIZE = 5;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        LinkedHashMap properties = new LinkedHashMap();
        this.sqlgGraph.tx().<SqlgVertex>streamingBatchMode(BATCH_SIZE, (e) -> {
            SqlgVertex previous = null;
            for (SqlgVertex person : e) {
                if (previous == null) {
                    previous = person;
                } else {
                    previous.streamFixedBatchEdge("friend", person);
                }
            }
            this.sqlgGraph.tx().flush();
        });
        for (int i = 1; i <= 10; i++) {
            String uuid1 = UUID.randomUUID().toString();
            properties.put("id", uuid1);
            this.sqlgGraph.streamVertexFixedBatch("Person", properties);
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();

        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("Person").count().next(), 0);
        Assert.assertEquals(8, this.sqlgGraph.traversal().E().hasLabel("friend").count().next(), 0);
    }

    @Test
    public void streamJava8StyleWithSchema() {
        List<String> uids = Arrays.asList("1", "2", "3", "4", "5");
        this.sqlgGraph.tx().streamingBatchMode(10);
        uids.stream().forEach(u -> this.sqlgGraph.streamVertexFixedBatch(T.label, "R_HG.Person", "name", u));
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(5, this.sqlgGraph.traversal().V().hasLabel("R_HG.Person").count().next(), 0l);
    }

    @Test
    public void streamBatchJava8Style() {
        List<String> uids = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            uids.add(String.valueOf(i));
        }
        AtomicInteger count = new AtomicInteger(0);
        BatchCallback<SqlgVertex> sqlgVertexBatchCallback = (v) -> {
            count.incrementAndGet();
        };
        this.sqlgGraph.tx().streamingBatchMode(10, (v) -> {
        });
        this.sqlgGraph.tx().streamingBatchMode(10, sqlgVertexBatchCallback);
        uids.stream().forEach(u -> this.sqlgGraph.streamVertexFixedBatch(T.label, "Person", "name", u));
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("Person").count().next(), 0l);
        Assert.assertEquals(10, count.get());
    }

    @Test
    public void testBatchContinuations() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Dog");
        v1.addEdge("pet", v2);
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingBatchMode(10);
        for (int i = 1; i <= 100; i++) {
            SqlgVertex v = this.sqlgGraph.streamVertexFixedBatch("Person", new LinkedHashMap<>());
        }
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingMode();
        this.sqlgGraph.streamVertex("Person", new LinkedHashMap<>());
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(102, this.sqlgGraph.traversal().V().hasLabel("Person").count().next(), 0l);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Dog").count().next(), 0l);
    }
}
