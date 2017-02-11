package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Date: 2015/11/21
 * Time: 2:24 PM
 */
public class TestGremlinCompileVertexStep extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        BaseTest.beforeClass();
        if (configuration.getString("jdbc.url").contains("postgresql")) {
            configuration.addProperty("distributed", true);
        }
    }

    @Test
    public void testVertexStep() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        testVertexStep_assert(this.sqlgGraph, a1, b1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testVertexStep_assert(this.sqlgGraph1, a1, b1);
        }
    }

    private void testVertexStep_assert(SqlgGraph sqlgGraph, Vertex a1, Vertex b1) {
        DefaultGraphTraversal<Vertex, Map<String, Object>> traversal = (DefaultGraphTraversal)sqlgGraph.traversal()
                .V(a1).as("a").local(__.out().as("b")).select("a", "b");
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Map<String, Object>> t = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertEquals(1, t.size());
        Assert.assertTrue(t.get(0).containsKey("a"));
        Assert.assertTrue(t.get(0).containsKey("b"));
        Assert.assertEquals(a1, t.get(0).get("a"));
        Assert.assertEquals(b1, t.get(0).get("b"));
    }
}
