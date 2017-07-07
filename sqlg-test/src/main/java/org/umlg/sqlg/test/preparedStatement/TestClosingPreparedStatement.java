package org.umlg.sqlg.test.preparedStatement;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2016/05/15
 * Time: 2:49 PM
 */
public class TestClosingPreparedStatement extends BaseTest {

//    @Test
//    public void testClosingPreparedStatement() {
//        this.sqlgGraph.addVertex(T.label, "A");
//        this.sqlgGraph.addVertex(T.label, "A");
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.traversal().V().next();
//        this.sqlgGraph.tx().commit();
//        assertTrue(this.sqlgGraph.tx().getPreparedStatementCache().isEmpty());
//        this.sqlgGraph.traversal().V().next();
//        this.sqlgGraph.tx().rollback();
//        assertTrue(this.sqlgGraph.tx().getPreparedStatementCache().isEmpty());
//    }

    @Test
    public void test() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for (int i = 0; i < 1_000_000; i++) {
            this.sqlgGraph.addVertex(T.label, "A");
            if (i % 10_000 == 0) {
                System.out.println(i);
                this.sqlgGraph.addVertex(T.label, "A", "name" + i, "asda");
            }
            if (i % 1000 == 0) {
                this.sqlgGraph.tx().commit();
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }
}
