package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;

/**
 * Date: 2015/10/21
 * Time: 8:18 PM
 */
public class TestRepeatStep extends BaseTest {

    @Test
    //This is not optimized
    public void testUntilRepeat() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        List<Vertex> t = this.sqlgGraph.traversal().V().repeat(__.out()).until(__.hasLabel("C")).toList();
        Assert.assertEquals(6, t.size());
        Assert.assertTrue(t.remove(c1));
        Assert.assertTrue(t.remove(c1));
        Assert.assertTrue(t.remove(c2));
        Assert.assertTrue(t.remove(c2));
        Assert.assertTrue(t.remove(c3));
        Assert.assertTrue(t.remove(c3));
        Assert.assertTrue(t.isEmpty());

        t = this.sqlgGraph.traversal().V().until(__.hasLabel("C")).repeat(__.out()).toList();
        Assert.assertEquals(9, t.size());
        Assert.assertTrue(t.remove(c1));
        Assert.assertTrue(t.remove(c1));
        Assert.assertTrue(t.remove(c1));
        Assert.assertTrue(t.remove(c2));
        Assert.assertTrue(t.remove(c2));
        Assert.assertTrue(t.remove(c2));
        Assert.assertTrue(t.remove(c3));
        Assert.assertTrue(t.remove(c3));
        Assert.assertTrue(t.remove(c3));
        Assert.assertTrue(t.isEmpty());
    }

    @Test
    //This is not optimized
    public void testRepeatWithUnoptimizableInternalSteps() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        List<Map<String, Vertex>> t = this.sqlgGraph.traversal().V().repeat(__.groupCount("m").by("name").out()).times(2).<Map<String, Vertex>>cap("m").toList();
        Assert.assertEquals(1, t.size());
        Assert.assertTrue(t.get(0).containsKey("a1"));
        Assert.assertTrue(t.get(0).containsKey("b1"));
        Assert.assertTrue(t.get(0).containsKey("b1"));
        Assert.assertTrue(t.get(0).containsKey("b1"));
        Assert.assertTrue(t.get(0).containsKey("c1"));
        Assert.assertTrue(t.get(0).containsKey("c1"));
        Assert.assertTrue(t.get(0).containsKey("c1"));

        Assert.assertEquals(1l, t.get(0).get("a1"));
        Assert.assertEquals(2l, t.get(0).get("b1"));
        Assert.assertEquals(2l, t.get(0).get("b2"));
        Assert.assertEquals(2l, t.get(0).get("b3"));
        Assert.assertEquals(2l, t.get(0).get("c1"));
        Assert.assertEquals(2l, t.get(0).get("c2"));
        Assert.assertEquals(2l, t.get(0).get("c3"));
    }

    @Test
    //This is not optimized
    public void testRepeatNoLimit() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().repeat(__.out("ab").out("bc")).toList();
        Assert.assertTrue(vertices.isEmpty());
    }

    @Test
    public void testRepeat() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out("ab").out("bc")).times(1).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(c1));
        Assert.assertTrue(vertices.contains(c2));
        Assert.assertTrue(vertices.contains(c3));

        vertices = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out("ab", "bc")).times(2).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(c1));
        Assert.assertTrue(vertices.contains(c2));
        Assert.assertTrue(vertices.contains(c3));

//        GraphTraversal<Vertex, Vertex> gt = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out("ab", "bc")).emit().times(2);
//        GraphTraversal<Vertex, Vertex> gt = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out("ab", "bc")).times(2).emit();
//        GraphTraversal<Vertex, Vertex> gt = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out("ab", "bc")).times(2);
    }

    @Test
    public void testTree() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();
        Tree tree = this.sqlgGraph.traversal().V().hasLabel("A").out().out().tree().next();
        System.out.println(tree);
        Assert.assertEquals(1, tree.size());
        Assert.assertTrue(tree.containsKey(a1));
        Tree tree2 = (Tree) tree.get(a1);
        Assert.assertEquals(1, tree2.size());
        Assert.assertTrue(tree2.containsKey(b1));
        Tree tree3 = (Tree) tree2.get(b1);
        Assert.assertEquals(3, tree3.size());
        Assert.assertTrue(tree3.containsKey(c1));
        Assert.assertTrue(tree3.containsKey(c1));
        Assert.assertTrue(tree3.containsKey(c1));
    }
}
