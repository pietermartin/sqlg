package org.umlg.sqlg.preparedstatement;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2017/01/15
 * Time: 8:50 PM
 */
public class TestPostgresPreparedStatement extends BaseTest {

    //Will throw PSQLException: ERROR: cached plan must not change result type if SqlgVertex.load uses SELECT * FROM
    @Test
    public void testSqlgVertexLoad() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        a.property("surname", "asdasd");
        Vertex b = this.sqlgGraph.addVertex(T.label, "A");
        b.property("a", "asdasd");
        Vertex c = this.sqlgGraph.addVertex(T.label, "A");
        c.property("b", "asdasd");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.traversal().V(a).next();
    }

    //Will throw PSQLException: ERROR: cached plan must not change result type if SqlgEdge.load uses SELECT * FROM
    @Test
    public void testSqlgEdgeLoad() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "halo");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "halo");
        Edge e1 = a.addEdge("ab", b);
        e1.property("a", "asdasd");
        Edge e2 = a.addEdge("ab", b);
        e2.property("b", "asdasd");
        Edge e3 = a.addEdge("ab", b);
        e3.property("c", "asdasd");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.traversal().E(e1).next();
    }
}
