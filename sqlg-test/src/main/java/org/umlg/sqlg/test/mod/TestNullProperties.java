package org.umlg.sqlg.test.mod;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgExceptions;
import org.umlg.sqlg.test.BaseTest;

public class TestNullProperties extends BaseTest {

    @Test
    public void testNullProperties() {
        try {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "John1", "surname", null, "age", 1);
        } catch (SqlgExceptions.InvalidPropertyTypeException e) {
            Assert.assertEquals("Property of type NULL is not supported", e.getMessage());
            this.sqlgGraph.tx().rollback();
        }
        try {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "John2", "surname", "Smith", "age", null);
        } catch (SqlgExceptions.InvalidPropertyTypeException e) {
            Assert.assertEquals("Property of type NULL is not supported", e.getMessage());
            this.sqlgGraph.tx().rollback();
        }
    }
}
