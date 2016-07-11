package org.umlg.sqlg.test.rollback;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.*;

/**
 * Date: 2014/09/24
 * Time: 2:15 PM
 */
public class TestRollback extends BaseTest {

    @Test
    public void shouldRollbackByDefault() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
//        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.traversal().tx().rollback();
        assertFalse(this.sqlgGraph.edges().hasNext());
    }

    @Test
    public void shouldHaveExceptionConsistencyWhenUsingManualTransactionOnRollback() {
        this.sqlgGraph.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
        try {
            this.sqlgGraph.tx().rollback();
            fail("An exception should be thrown when read/write behavior is manual and no transaction is opened");
        } catch (Exception ex) {
            validateException(Transaction.Exceptions.transactionMustBeOpenToReadWrite(), ex);
        }
    }

    @Test
    public void testRollback() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.tx().commit();
        v1.property("name", "john");
        this.sqlgGraph.tx().rollback();
        assertFalse(v1.property("name").isPresent());
    }

    public static void validateException(final Throwable expected, final Throwable actual) {
        System.out.println(actual);
    }
}
