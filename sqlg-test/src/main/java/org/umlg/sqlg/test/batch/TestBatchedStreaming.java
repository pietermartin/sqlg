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
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

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
        int BATCH_SIZE = 100;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        LinkedHashMap properties = new LinkedHashMap();
        this.sqlgGraph.tx().streamingWithLockBatchModeOn();
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
            SqlgVertex v1 = this.sqlgGraph.streamVertexWithLock("Person", properties);
            properties.put("id", uuid2);
            SqlgVertex v2 = this.sqlgGraph.streamVertexWithLock("Person", properties);
            uids.add(Pair.of(v1, v2));
            if (i % (BATCH_SIZE / 2) == 0) {
                this.sqlgGraph.tx().flush();
                this.sqlgGraph.tx().streamingWithLockBatchModeOn();
                for (Pair<SqlgVertex, SqlgVertex> uid : uids) {
                    uid.getLeft().streamEdgeWithLock("friend", uid.getRight());
                }
                //This is needed because the number of edges are less than the batch size so it will not be auto flushed
                this.sqlgGraph.tx().flush();
                uids.clear();
                this.sqlgGraph.tx().streamingWithLockBatchModeOn();
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
        this.sqlgGraph.tx().streamingWithLockBatchModeOn();
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
            SqlgVertex v1 = this.sqlgGraph.streamVertexWithLock("A.Person", properties);
            properties.put("id", uuid2);
            SqlgVertex v2 = this.sqlgGraph.streamVertexWithLock("A.Person", properties);
            uids.add(Pair.of(v1, v2));
            if (i % (BATCH_SIZE / 2) == 0) {
                this.sqlgGraph.tx().flush();
                for (Pair<SqlgVertex, SqlgVertex> uid : uids) {
                    uid.getLeft().streamEdgeWithLock("friend", uid.getRight());
                }
                //This is needed because the number of edges are less than the batch size so it will not be auto flushed
                this.sqlgGraph.tx().flush();
                uids.clear();
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
        LinkedHashMap properties = new LinkedHashMap();
        List<SqlgVertex> persons = new ArrayList<>();
        this.sqlgGraph.tx().streamingWithLockBatchModeOn();
        for (int i = 1; i <= 10; i++) {
            String uuid1 = UUID.randomUUID().toString();
            properties.put("id", uuid1);
            persons.add(this.sqlgGraph.streamVertexWithLock("Person", properties));
        }
        this.sqlgGraph.tx().flush();
        SqlgVertex previous = null;
        for (SqlgVertex person : persons) {
            if (previous == null) {
                previous = person;
            } else {
                previous.streamEdgeWithLock("friend", person);
            }
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("Person").count().next(), 0);
        Assert.assertEquals(9, this.sqlgGraph.traversal().E().hasLabel("friend").count().next(), 0);
    }

    @Test
    public void streamJava8StyleWithSchema() {
        List<String> uids = Arrays.asList("1", "2", "3", "4", "5");
        this.sqlgGraph.tx().streamingBatchModeOn();
        uids.stream().forEach(u -> this.sqlgGraph.streamVertex(T.label, "R_HG.Person", "name", u));
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(5, this.sqlgGraph.traversal().V().hasLabel("R_HG.Person").count().next(), 0l);
    }


    @Test
    public void testBatchContinuations() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Dog");
        v1.addEdge("pet", v2);
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingWithLockBatchModeOn();
        for (int i = 1; i <= 100; i++) {
            SqlgVertex v = this.sqlgGraph.streamVertexWithLock("Person", new LinkedHashMap<>());
        }
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.streamVertex("Person", new LinkedHashMap<>());
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(102, this.sqlgGraph.traversal().V().hasLabel("Person").count().next(), 0l);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Dog").count().next(), 0l);
    }
}
