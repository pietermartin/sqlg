package org.umlg.sqlg.test.batch;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.test.BaseTest;

import java.util.LinkedHashMap;

/**
 * Date: 2015/05/19
 * Time: 9:34 PM
 */
public class TestBatchCompleteVertex extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotQueryWhileStreaming() {
        this.sqlgGraph.tx().streamingBatchMode();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex("Person");
        }
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("Person").count().next(), 1);
        this.sqlgGraph.tx().commit();
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotQueryFromVertexWhileStreaming() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchMode();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex("Person");
        }
        Assert.assertEquals(100, this.sqlgGraph.traversal().V(v1).out("friend").count().next(), 1);
        this.sqlgGraph.tx().commit();
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotQueryFromVertexWhileStreaming2() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchMode();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex("Person");
        }
        Assert.assertEquals(100, IteratorUtils.count(v1.edges(Direction.OUT, "friend")), 1);
        this.sqlgGraph.tx().commit();
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotQueryFromVertexWhileStreaming3() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchMode();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex("Person");
        }
        Assert.assertEquals(100, IteratorUtils.count(v1.vertices(Direction.OUT, "friend")), 1);
        this.sqlgGraph.tx().commit();
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotQueryFromGraphVerticesWhileStreaming() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchMode();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex("Person");
        }
        Assert.assertEquals(102, IteratorUtils.count(this.sqlgGraph.vertices()), 1);
        this.sqlgGraph.tx().commit();
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotQueryFromGraphEdgesWhileStreaming() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchMode();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex("Person");
        }
        Assert.assertEquals(102, IteratorUtils.count(this.sqlgGraph.edges()), 1);
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testVertexWithNoProperties() {
        this.sqlgGraph.tx().streamingBatchMode();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex("Person");
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("Person").count().next(), 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotAddVertexOnceStreaming() {
        this.sqlgGraph.tx().streamingBatchMode();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "test");
        SqlgVertex v2 = (SqlgVertex)this.sqlgGraph.addVertex("A", keyValues);
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testCompleteVertexChecksSingleLabelOnly() {
        this.sqlgGraph.tx().streamingBatchMode();
        LinkedHashMap<String, Object> keyValue = new LinkedHashMap<>();
        keyValue.put("name", "a");
        keyValue.put("surname", "b");
        this.sqlgGraph.streamVertex("Person", keyValue);
        this.sqlgGraph.streamVertex("Persons", keyValue);
        this.sqlgGraph.tx().commit();
        Assert.fail();
    }

    @Test
    public void testCompleteVertexFlushAndCloseStream() {
        this.sqlgGraph.tx().streamingBatchMode();
        LinkedHashMap<String, Object> keyValue = new LinkedHashMap<>();
        keyValue.put("name", "a");
        keyValue.put("surname", "b");
        this.sqlgGraph.streamVertex("Person", keyValue);
        this.sqlgGraph.flushAndCloseStream();
        this.sqlgGraph.streamVertex("Persons", keyValue);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Person").count().next(), 0l);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Persons").count().next(), 0l);
        Assert.assertEquals("a", this.sqlgGraph.traversal().V().hasLabel("Person").next().<String>value("name"));
        Assert.assertEquals("b", this.sqlgGraph.traversal().V().hasLabel("Person").next().<String>value("surname"));
        Assert.assertEquals("a", this.sqlgGraph.traversal().V().hasLabel("Persons").next().<String>value("name"));
        Assert.assertEquals("b", this.sqlgGraph.traversal().V().hasLabel("Persons").next().<String>value("surname"));
    }

    @Test(expected = IllegalStateException.class)
    public void testCompleteVertexChecksSameKeys() {
        this.sqlgGraph.tx().streamingBatchMode();
        LinkedHashMap<String, Object> keyValue = new LinkedHashMap<>();
        keyValue.put("name", "a");
        keyValue.put("surname", "b");
        this.sqlgGraph.streamVertex("Person", keyValue);
        keyValue = new LinkedHashMap<>();
        keyValue.put("namea", "a");
        keyValue.put("surname", "b");
        this.sqlgGraph.streamVertex("Person", keyValue);
        this.sqlgGraph.tx().commit();
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testStreamingVertexKeysSameOrder() {
        this.sqlgGraph.tx().streamingBatchMode();
        LinkedHashMap<String, Object> keyValue = new LinkedHashMap<>();
        keyValue.put("name", "a");
        keyValue.put("surname", "b");
        this.sqlgGraph.streamVertex("Person", keyValue);
        keyValue = new LinkedHashMap<>();
        keyValue.put("surname", "b");
        keyValue.put("name", "a");
        this.sqlgGraph.streamVertex("Person", keyValue);
        this.sqlgGraph.tx().commit();
        Assert.fail();
    }

    @Test
    public void testMilCompleteVertex() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().streamingBatchMode();
        for (int i = 1; i < 1000001; i++) {
            LinkedHashMap<String, Object> keyValue = new LinkedHashMap<>();
            for (int j = 0; j < 2; j++) {
                keyValue.put("name" + j, "a" + i);
            }
            this.sqlgGraph.streamVertex("Person", keyValue);
            if (i % 250000 == 0) {
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().streamingBatchMode();
                System.out.println(i);
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        Assert.assertEquals(1000000l, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next().longValue());
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }

    @Test
    public void testStreamingRollback() {
        this.sqlgGraph.tx().streamingBatchMode();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        keyValues.put("surname", "halo");
        for (int i = 0; i < 1000; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Man", keyValues);
        }
        this.sqlgGraph.flushAndCloseStream();
        for (int i = 0; i < 1000; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Female", keyValues);
        }
        this.sqlgGraph.tx().rollback();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
    }

//    Next two should move to benchmark tests
//    @Test
//    public void testVerticesStream() {
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        this.sqlgGraph.tx().streamingBatchMode();
//        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
//        keyValues.put("name", "halo");
//        keyValues.put("surname", "halo");
//        for (int i = 0; i < 10000000; i++) {
//            keyValues.put("age", i);
//            this.sqlgGraph.streamVertex("Man", keyValues);
//        }
//        this.sqlgGraph.flushAndCloseStream();
//        for (int i = 0; i < 10000000; i++) {
//            keyValues.put("age", i);
//            this.sqlgGraph.streamVertex("Female", keyValues);
//        }
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println(stopWatch.toString());
//    }
//
//    @Test
//    public void testVertices() {
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        this.sqlgGraph.tx().batchModeOn();
//        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
//        keyValues.put("name", "halo");
//        keyValues.put("surname", "halo");
//        for (int i = 1; i <= 10000000; i++) {
//            keyValues.put("age", i);
//            this.sqlgGraph.addVertex("Man", keyValues);
//            if (i % 100000 == 0) {
//                this.sqlgGraph.tx().commit();
//                this.sqlgGraph.tx().batchModeOn();
//            }
//        }
//        for (int i = 1; i <= 10000000; i++) {
//            keyValues.put("age", i);
//            this.sqlgGraph.addVertex("Female", keyValues);
//            if (i % 100000 == 0) {
//                this.sqlgGraph.tx().commit();
//                this.sqlgGraph.tx().batchModeOn();
//            }
//        }
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println(stopWatch.toString());
//    }
}
