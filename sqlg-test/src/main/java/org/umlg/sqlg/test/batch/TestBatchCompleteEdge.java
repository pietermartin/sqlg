package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2015/07/18
 * Time: 4:18 PM
 */
public class TestBatchCompleteEdge extends BaseTest {

    final private int NUMBER_OF_VERTICES = 10000;

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testCanNotCreateBatchEdgeWhileBatchVertexInProgress() {
        this.sqlgGraph.tx().batchModeOn(true);
        SqlgVertex  v1 = (SqlgVertex)this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex v2 = (SqlgVertex)this.sqlgGraph.addVertex(T.label, "A");
        v1.streamEdge("a", v2);
        v1.streamEdge("b", v2);
        Assert.fail();
    }

//    @Test(expected = IllegalStateException.class)
//    public void testEdgeLabelRemainsTheSame() {
//        SqlgVertex  v1 = (SqlgVertex)this.sqlgGraph.addVertex(T.label, "A");
//        SqlgVertex v2 = (SqlgVertex)this.sqlgGraph.addVertex(T.label, "A");
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().batchModeOn(true);
//        v1.streamEdge("a", v2);
//        v1.streamEdge("b", v2);
//        Assert.fail();
//    }
//
//    @Test(expected = IllegalStateException.class)
//    public void testEdgePropertiesRemainsTheSame() {
//        SqlgVertex  v1 = (SqlgVertex)this.sqlgGraph.addVertex(T.label, "A");
//        SqlgVertex v2 = (SqlgVertex)this.sqlgGraph.addVertex(T.label, "A");
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().batchModeOn(true);
//        v1.streamEdge("a", v2, "name", "halo");
//        v1.streamEdge("a", v2, "namea", "halo");
//        Assert.fail();
//    }
//
//    @Test
//    public void testMilCompleteEdges() {
//        ArrayList<SqlgVertex> persons = createMilPersonVertex();
//        ArrayList<SqlgVertex> cars = createMilCarVertex();
//        this.sqlgGraph.tx().commit();
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        this.sqlgGraph.tx().batchModeOn(true);
//        for (int i = 0; i < NUMBER_OF_VERTICES; i++) {
//            SqlgVertex person = persons.get(0);
//            SqlgVertex car = cars.get(i);
//            person.streamEdge("person_car", car, "name1", "halo", "name2", "halo");
//        }
//        this.sqlgGraph.tx().commit();
//        Assert.assertEquals(NUMBER_OF_VERTICES, this.sqlgGraph.traversal().V(persons.get(0)).out("person_car").toList().size());
//        stopWatch.stop();
//        System.out.println("testMilCompleteEdges took " + stopWatch.toString());
//    }
//
//    private ArrayList<SqlgVertex> createMilPersonVertex() {
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        ArrayList<SqlgVertex> result = new ArrayList<>();
//        this.sqlgGraph.tx().batchModeOn();
//        for (int i = 1; i < NUMBER_OF_VERTICES + 1; i++) {
//            Map<String, Object> keyValue = new LinkedHashMap<>();
//            for (int j = 0; j < 100; j++) {
//                keyValue.put("name" + j, "aaaaaaaaaa" + i);
//            }
//            SqlgVertex person = (SqlgVertex) this.sqlgGraph.addVertex("Person", keyValue);
//            result.add(person);
//            if (i % (NUMBER_OF_VERTICES / 10) == 0) {
//                this.sqlgGraph.tx().commit();
//                this.sqlgGraph.tx().batchModeOn();
//            }
//        }
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println("createMilPersonVertex took " + stopWatch.toString());
//        return result;
//    }
//
//    private ArrayList<SqlgVertex> createMilCarVertex() {
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        ArrayList<SqlgVertex> result = new ArrayList<>();
//        this.sqlgGraph.tx().batchModeOn();
//        for (int i = 1; i < NUMBER_OF_VERTICES + 1; i++) {
//            Map<String, Object> keyValue = new LinkedHashMap<>();
//            for (int j = 0; j < 100; j++) {
//                keyValue.put("name" + j, "aaaaaaaaaa" + i);
//            }
//            SqlgVertex car = (SqlgVertex) this.sqlgGraph.addVertex("Car", keyValue);
//            result.add(car);
//            if (i % (NUMBER_OF_VERTICES / 10) == 0) {
//                this.sqlgGraph.tx().commit();
//                this.sqlgGraph.tx().batchModeOn();
//            }
//        }
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println("createMilCarVertex took " + stopWatch.toString());
//        return result;
//    }
}
