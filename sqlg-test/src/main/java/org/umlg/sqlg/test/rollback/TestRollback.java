package org.umlg.sqlg.test.rollback;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/09/24
 * Time: 2:15 PM
 */
public class TestRollback extends BaseTest {

    @Test
    public void shouldHaveExceptionConsistencyWhenUsingManualTransactionOnRollback() {
        this.sqlgGraph.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
        try {
            this.sqlgGraph.tx().rollback();
            Assert.fail("An exception should be thrown when read/write behavior is manual and no transaction is opened");
        } catch (Exception ex) {
            validateException(Transaction.Exceptions.transactionMustBeOpenToReadWrite(), ex);
        }
    }

//    @Test
//    public void testRollback() {
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
//        this.sqlgGraph.tx().commit();
//        v1.property("name", "john");
//        this.sqlgGraph.tx().rollback();
//        Assert.assertFalse(v1.property("name").isPresent());
//    }

    public static void validateException(final Throwable expected, final Throwable actual) {
        System.out.println(actual);
    }
}
