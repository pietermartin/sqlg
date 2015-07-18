package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2015/01/19
 * Time: 6:22 AM
 */
public class TestGremlinCompileWithOrderBy extends BaseTest {

    @Test
    public void testHasLabelOut() {
//        SqlgVertex a1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
//        SqlgVertex b1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        a1.addEdge("outB", b1);
//        this.sqlgGraph.tx().commit();
//        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().both().has(T.label, "B");
//        printTraversalForm(traversal);
//        List<Vertex> softwares = traversal.toList();
//        Assert.assertEquals(1, softwares.size());
//        for (Vertex software : softwares) {
//            if (!software.label().equals("B")) {
//                Assert.fail("expected label B found " + software.label());
//            }
//        }
//        .outE(this.getLabel()) .as("edge")
//                .order().by(BaseCollection.IN_EDGE_SEQUENCE_ID, Order.incr)
//                .inV()
//                .as("vertex")
//                .select();
    }

}
