package org.umlg.sqlg.test.index;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.lang.time.StopWatch;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2014/10/02
 * Time: 8:04 AM
 */
public class TestForeignKeyIndexPerformance extends BaseTest {

    @Test
    public void testForeignKeyPerformance() {
        this.sqlG.tx().batchModeOn();
        Vertex v1 = this.sqlG.addVertex(T.label, "Person");
        for (int i = 0; i < 10000; i++) {
            v1.addEdge("car", this.sqlG.addVertex(T.label, "Car"));
        }
        this.sqlG.tx().commit();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<Vertex> cars = v1.out("car").toList();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        Assert.assertEquals(10000, cars.size());
    }
}
