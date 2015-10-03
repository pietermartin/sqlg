package org.umlg.sqlg.test.batch;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 2015/07/18
 * Time: 4:18 PM
 */
public class TestStreamingEdge extends BaseTest {

    final private int NUMBER_OF_VERTICES = 10000;

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotCreateBatchEdgeWhileBatchVertexInProgress() {
        SqlgVertex v1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "Dog");
        SqlgVertex v2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "House");
        this.sqlgGraph.tx().streamingMode();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "test");
        this.sqlgGraph.streamVertex("A", keyValues);
        this.sqlgGraph.streamVertex("A", keyValues);
        v1.streamEdge("a", v2);
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testEdgeLabelRemainsTheSame() {
        SqlgVertex v1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex v2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingMode();
        v1.streamEdge("a", v2);
        v1.streamEdge("b", v2);
        Assert.fail();
    }

    @Test
    public void testEdgeFlushAndCloseStream() {
        SqlgVertex v1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex v2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingMode();
        v1.streamEdge("a", v2);
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingMode();
        v1.streamEdge("b", v2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("a").count().next(), 1);
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("b").count().next(), 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testEdgePropertiesRemainsTheSame() {
        SqlgVertex v1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex v2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingMode();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        v1.streamEdge("a", v2, keyValues);
        keyValues.clear();
        keyValues.put("namea", "halo");
        v1.streamEdge("a", v2, keyValues);
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testEdgePropertiesSameOrder() {
        SqlgVertex v1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex v2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingMode();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        keyValues.put("surname", "test");
        v1.streamEdge("a", v2, keyValues);
        keyValues.clear();
        keyValues.put("surname", "test");
        keyValues.put("name", "halo");
        v1.streamEdge("a", v2, keyValues);
        Assert.fail();
    }


    @Test
    public void testStreamingVerticesAndEdges() {
        this.sqlgGraph.tx().streamingMode();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        keyValues.put("surname", "halo");
        for (int i = 0; i < 1000; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Man", keyValues);
        }
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingMode();
        for (int i = 0; i < 1000; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Female", keyValues);
        }
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingMode();
        int count = 0;
        List<Vertex> men = this.sqlgGraph.traversal().V().hasLabel("Man").toList();
        List<Vertex> females = this.sqlgGraph.traversal().V().hasLabel("Female").toList();
        for (Vertex man : men) {
            SqlgVertex female = (SqlgVertex) females.get(count++);
            ((SqlgVertex)man).streamEdge("married", female);
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1000, this.sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(1000, this.sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(1000, this.sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
    }

    @Test
    public void testMilCompleteEdges() {
        ArrayList<SqlgVertex> persons = createMilPersonVertex();
        ArrayList<SqlgVertex> cars = createMilCarVertex();
        this.sqlgGraph.tx().commit();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().streamingMode();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        keyValues.put("name2", "halo");
        for (int i = 0; i < NUMBER_OF_VERTICES; i++) {
            SqlgVertex person = persons.get(0);
            SqlgVertex car = cars.get(i);
            person.streamEdge("person_car", car, keyValues);
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(NUMBER_OF_VERTICES, this.sqlgGraph.traversal().V(persons.get(0)).out("person_car").toList().size());
        stopWatch.stop();
        System.out.println("testMilCompleteEdges took " + stopWatch.toString());
    }

    private ArrayList<SqlgVertex> createMilPersonVertex() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        ArrayList<SqlgVertex> result = new ArrayList<>();
        this.sqlgGraph.tx().batchModeOn();
        for (int i = 1; i < NUMBER_OF_VERTICES + 1; i++) {
            Map<String, Object> keyValue = new LinkedHashMap<>();
            for (int j = 0; j < 100; j++) {
                keyValue.put("name" + j, "aaaaaaaaaa" + i);
            }
            SqlgVertex person = (SqlgVertex) this.sqlgGraph.addVertex("Person", keyValue);
            result.add(person);
            if (i % (NUMBER_OF_VERTICES / 10) == 0) {
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().batchModeOn();
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println("createMilPersonVertex took " + stopWatch.toString());
        return result;
    }

    private ArrayList<SqlgVertex> createMilCarVertex() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        ArrayList<SqlgVertex> result = new ArrayList<>();
        this.sqlgGraph.tx().batchModeOn();
        for (int i = 1; i < NUMBER_OF_VERTICES + 1; i++) {
            Map<String, Object> keyValue = new LinkedHashMap<>();
            for (int j = 0; j < 100; j++) {
                keyValue.put("name" + j, "aaaaaaaaaa" + i);
            }
            SqlgVertex car = (SqlgVertex) this.sqlgGraph.addVertex("Car", keyValue);
            result.add(car);
            if (i % (NUMBER_OF_VERTICES / 10) == 0) {
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().batchModeOn();
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println("createMilCarVertex took " + stopWatch.toString());
        return result;
    }

}
