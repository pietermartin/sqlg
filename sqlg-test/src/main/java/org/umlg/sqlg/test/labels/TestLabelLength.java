package org.umlg.sqlg.test.labels;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgExceptions;
import org.umlg.sqlg.test.BaseTest;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/06/30
 */
public class TestLabelLength extends BaseTest {

    @Test(expected = SqlgExceptions.InvalidTableException.class)
    public void testLabelLength() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().isPostgresql());
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "B");
        v1.addEdge("A12345678901234567890123456789012345678901234567890123456789halo", v2);
        this.sqlgGraph.tx().commit();
    }

    @Test(expected = SqlgExceptions.InvalidColumnException.class)
    public void testColumnLength() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().isPostgresql());
        this.sqlgGraph.addVertex(T.label, "A", "A12345678901234567890123456789012345678901234567890123456789halo", "a");
        this.sqlgGraph.tx().commit();
    }
}
