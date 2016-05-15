package org.umlg.sqlg.test.preparedStatement;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertTrue;

/**
 * Date: 2016/05/15
 * Time: 2:49 PM
 */
public class TestClosingPreparedStatement extends BaseTest {

    @Test
    public void testClosingPreparedStatement() {
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.traversal().V().next();
        this.sqlgGraph.tx().commit();
        assertTrue(this.sqlgGraph.tx().getPreparedStatementCache().isEmpty());
        this.sqlgGraph.traversal().V().next();
        this.sqlgGraph.tx().rollback();
        assertTrue(this.sqlgGraph.tx().getPreparedStatementCache().isEmpty());
    }
}
