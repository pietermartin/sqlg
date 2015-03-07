package org.umlg.sqlg.test.vertex;

import org.apache.tinkerpop.gremlin.process.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgExceptions;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2015/03/05
 * Time: 3:03 PM
 */
public class TestVertexLabelSize extends BaseTest {

    @Test(expected = SqlgExceptions.InvalidSchemaException.class)
    public void testLargeSchemaNameSize() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggg.a");
        this.sqlgGraph.tx().commit();
    }

    @Test(expected = SqlgExceptions.InvalidTableException.class)
    public void testLargeTableNameSize() {
       Vertex v = this.sqlgGraph.addVertex(T.label, "a.aaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggg");
        this.sqlgGraph.tx().commit();
    }

    @Test(expected = SqlgExceptions.InvalidColumnException.class)
    public void testLargeColumnNameSize() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "a.aaaaaaaaabbbbbbbbbbccccccccccddddddddd",
                "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffggggggggggaaaa", "a");
        this.sqlgGraph.tx().commit();
    }
}
