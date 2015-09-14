package org.umlg.sqlg.test.batch;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.test.BaseTest;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Date: 2015/05/19
 * Time: 9:34 PM
 */
public class TestBatchCompleteVertex extends BaseTest {

    @Test
    public void testCompleteVertex() {
        this.sqlgGraph.tx().batchModeOn(true);
        Map<String, Object> keyValue = new LinkedHashMap<>();
        keyValue.put("name", "a");
        keyValue.put("surname", "b");
        this.sqlgGraph.addCompleteVertex("Person", keyValue);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1l, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next().longValue());
    }

//    @Test
    public void testMilCompleteVertex() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().batchModeOn(true);
        for (int i = 1; i < 1000001; i++) {
            Map<String, Object> keyValue = new LinkedHashMap<>();
            for (int j = 0; j < 2; j++) {
                keyValue.put("name" + j, "a" + i);
            }
            SqlgVertex person = (SqlgVertex) this.sqlgGraph.addCompleteVertex("Person", keyValue);
            if (i % 250000 == 0) {
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().batchModeOn(true);
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

}
