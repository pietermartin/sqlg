package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Date: 2014/07/13
 * Time: 3:38 PM
 */
public class TestAllVertices extends BaseTest {

//    @Test
//    public void testAllVertices()  {
//        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
//        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
//        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
//
//        Vertex washineMachine = this.sqlgGraph.addVertex(T.label, "Product", "productName", "Washing Machine");
//        marko.addEdge("happiness", washineMachine, "love", true);
//
//
//        this.sqlgGraph.tx().commit();
//        Assert.assertEquals(4L, this.sqlgGraph.traversal().V().count().next(), 0);
//    }

    @Test
    public void testVertexIterator() {
        Vertex a = this.sqlgGraph.addVertex();
        Vertex b = this.sqlgGraph.addVertex();
        Vertex c = this.sqlgGraph.addVertex();
        Vertex d = this.sqlgGraph.addVertex();
        a.addEdge("aaa", b);
        b.addEdge("aaa", b);
        c.addEdge("aaa", b);
        d.addEdge("aaa", b);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertexes = this.sqlgGraph.traversal().V().both().toList();
        Assert.assertEquals(8, vertexes.size());
    }

//    @Test
//    public void testVertexIteratorWithIncorrectId() throws Exception {
//        Graph g = this.sqlgGraph;
//        final Vertex v1 = g.addVertex("name", "marko");
//        final Object oid = v1.id();
//        g.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);
//        g.close();
//
//        g = SqlgGraph.open(configuration);
//        try {
//            g.vertices(oid).next();
//            fail("Vertex should not be found as close behavior was set to rollback");
//        } catch (Exception ex) {
//            validateException(Graph.Exceptions.elementNotFound(Vertex.class, oid), ex);
//        }
//    }
//
//    public static void validateException(final Throwable expected, final Throwable actual) {
//        assertThat(actual, instanceOf(expected.getClass()));
//    }
}
