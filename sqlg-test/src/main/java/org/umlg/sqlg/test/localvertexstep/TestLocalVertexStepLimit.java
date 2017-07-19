package org.umlg.sqlg.test.localvertexstep;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2017/03/29
 * Time: 10:33 PM
 */
public class TestLocalVertexStepLimit extends BaseTest {

    //This is not optimized as LocalSteps with a local traversal with a reducing barrier step is not optimized.
    @Test
    public void testCount() {
        loadModern();
        DefaultTraversal<Vertex, Long> traversal = (DefaultTraversal<Vertex, Long>) this.sqlgGraph.traversal()
                .V()
                .local(
                        __.out().count()
                );
        List<Long> counts = traversal.toList();
//        for (Long count : counts) {
//            System.out.println(count);
//        }
        Assert.assertEquals(6, counts.size());
        Assert.assertTrue(counts.remove(3L));
        Assert.assertTrue(counts.remove(2L));
        Assert.assertTrue(counts.remove(1L));
        Assert.assertTrue(counts.remove(0L));
        Assert.assertTrue(counts.remove(0L));
        Assert.assertTrue(counts.remove(0L));
        Assert.assertTrue(counts.isEmpty());
    }

    @Test
    public void g_V_localXoutE_limitX1X_inVX_limitX3X() {
        loadModern();
        final Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal()
                .V().local(
                        __.outE().limit(1)
                ).inV().limit(3);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        Assert.assertEquals(3, counter);
    }

    @Test
    public void testLocalStepLimitSingleQuery() {
        for (int i = 0; i < 10; i++) {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a" + i, "age", i);

            Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "b_" + i, "order", 1);
            Vertex bb = this.sqlgGraph.addVertex(T.label, "BB", "name", "bb_" + i, "order", 2);
            a.addEdge("ab", b);
            a.addEdge("abb", bb);
            b = this.sqlgGraph.addVertex(T.label, "B", "name", "b_" + i, "order", 3);
            bb = this.sqlgGraph.addVertex(T.label, "BB", "name", "bb_" + i, "order", 4);
            a.addEdge("ab", b);
            a.addEdge("abb", bb);
            b = this.sqlgGraph.addVertex(T.label, "B", "name", "b_" + i, "order", 5);
            bb = this.sqlgGraph.addVertex(T.label, "BB", "name", "bb_" + i, "order", 6);
            a.addEdge("ab", b);
            a.addEdge("abb", bb);
            b = this.sqlgGraph.addVertex(T.label, "B", "name", "b_" + i, "order", 7);
            bb = this.sqlgGraph.addVertex(T.label, "BB", "name", "bb_" + i, "order", 8);
            a.addEdge("ab", b);
            a.addEdge("abb", bb);
            b = this.sqlgGraph.addVertex(T.label, "B", "name", "b_" + i, "order", 9);
            bb = this.sqlgGraph.addVertex(T.label, "BB", "name", "bb_" + i, "order", 10);
            a.addEdge("ab", b);
            a.addEdge("abb", bb);
        }

        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .order().by("age")
                .local(
                        __.out()
                                .order().by("order")
                                .range(2, 4)
                ).toList();
        Assert.assertEquals(20, vertices.size());
        Assert.assertEquals("b_0", vertices.get(0).value("name"));
        Assert.assertEquals("bb_0", vertices.get(1).value("name"));
        Assert.assertEquals("b_1", vertices.get(2).value("name"));
        Assert.assertEquals("bb_1", vertices.get(3).value("name"));
        Assert.assertEquals("b_2", vertices.get(4).value("name"));
        Assert.assertEquals("bb_2", vertices.get(5).value("name"));
        Assert.assertEquals("b_3", vertices.get(6).value("name"));
        Assert.assertEquals("bb_3", vertices.get(7).value("name"));
        Assert.assertEquals("b_4", vertices.get(8).value("name"));
        Assert.assertEquals("bb_4", vertices.get(9).value("name"));
        Assert.assertEquals("b_5", vertices.get(10).value("name"));
        Assert.assertEquals("bb_5", vertices.get(11).value("name"));
        Assert.assertEquals("b_6", vertices.get(12).value("name"));
        Assert.assertEquals("bb_6", vertices.get(13).value("name"));
        Assert.assertEquals("b_7", vertices.get(14).value("name"));
        Assert.assertEquals("bb_7", vertices.get(15).value("name"));
        Assert.assertEquals("b_8", vertices.get(16).value("name"));
        Assert.assertEquals("bb_8", vertices.get(17).value("name"));
        Assert.assertEquals("b_9", vertices.get(18).value("name"));
        Assert.assertEquals("bb_9", vertices.get(19).value("name"));
    }

}
