package org.umlg.sqlg.test.rollback;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.apache.tinkerpop.gremlin.AbstractGremlinTest.assertVertexEdgeCounts;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Date: 2014/09/24
 * Time: 2:15 PM
 */
public class TestRollback extends BaseTest {

    @Test
    public void shouldRollbackElementAutoTransactionByDefault() {
        assertVertexEdgeCounts(this.sqlgGraph, 0, 0);
        Vertex v1 = this.sqlgGraph.addVertex();
        @SuppressWarnings("UnusedAssignment")
        Edge e1 = v1.addEdge("l", v1);
        this.sqlgGraph.tx().commit();
        assertVertexEdgeCounts(this.sqlgGraph, 1, 1);
        v1.remove();
        this.sqlgGraph.tx().commit();
        assertVertexEdgeCounts(this.sqlgGraph, 0, 0);

        v1 = this.sqlgGraph.addVertex();
        e1 = v1.addEdge("l", v1);
        assertVertexEdgeCounts(this.sqlgGraph, 1, 1);
        assertEquals(v1.id(), this.sqlgGraph.vertices(v1.id()).next().id());
        assertEquals(e1.id(), this.sqlgGraph.edges(e1.id()).next().id());
        this.sqlgGraph.traversal().tx().rollback();
        assertVertexEdgeCounts(this.sqlgGraph, 0, 0);
    }

    /**
     * This test is for HSQLDB in particular.
     * HSQLDB auto commits transactions that execute schema creation commands.
     * This invalidates the rollback logic that the test is trying to tests.
     * So first create, commit delete and then test rollback.
     */
    @Test
    public void shouldRollbackByDefault() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        a1.remove();
        b1.remove();
        this.sqlgGraph.tx().commit();
        assertFalse(this.sqlgGraph.edges().hasNext());
        assertFalse(this.sqlgGraph.vertices().hasNext());

        a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().rollback();
        assertFalse(this.sqlgGraph.edges().hasNext());
        assertFalse(this.sqlgGraph.vertices().hasNext());
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
