package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.DefaultSqlgTraversal;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2015/02/20
 * Time: 8:05 PM
 */
public class TestGremlinCompileGraphV extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Test
    public void testGraphStepWithAs() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        testGraphStepWithAs_aasert(this.sqlgGraph, a1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testGraphStepWithAs_aasert(this.sqlgGraph1, a1);
        }
    }

    private void testGraphStepWithAs_aasert(SqlgGraph sqlgGraph, Vertex a1) {
        DefaultSqlgTraversal<Vertex, Path> traversal = (DefaultSqlgTraversal<Vertex, Path>) sqlgGraph.traversal().V(a1).as("a").out().as("b").out().path();
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Path> result = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(1, result.size());
    }

    @Test
    public void testGraphVHas() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");

        a1.addEdge("b", b1);
        a1.addEdge("b", b2);
        a1.addEdge("b", b3);
        a1.addEdge("b", b4);

        a2.addEdge("b", b1);
        a2.addEdge("b", b2);
        a2.addEdge("b", b3);
        a2.addEdge("b", b4);

        this.sqlgGraph.tx().commit();

        testGraphVHas_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testGraphVHas_assert(this.sqlgGraph1);
        }
    }

    private void testGraphVHas_assert(SqlgGraph sqlgGraph) {
        DefaultSqlgTraversal<Vertex, Vertex> traversal = (DefaultSqlgTraversal<Vertex, Vertex>) sqlgGraph.traversal().V().has(T.label, "A").out("b");
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> bs = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(8, bs.size());
    }
}
