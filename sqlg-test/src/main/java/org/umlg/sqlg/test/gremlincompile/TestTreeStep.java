package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2015/10/21
 * Time: 7:39 PM
 */
public class TestTreeStep extends BaseTest {

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
        Tree tree2  = (Tree) tree.get(a1);
        Assert.assertEquals(1, tree2.size());
        Assert.assertTrue(tree2.containsKey(b1));
        Tree tree3  = (Tree) tree2.get(b1);
        Assert.assertEquals(3, tree3.size());
        Assert.assertTrue(tree3.containsKey(c1));
        Assert.assertTrue(tree3.containsKey(c1));
        Assert.assertTrue(tree3.containsKey(c1));

        //left join todo
//        GraphTraversal gt = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out("ab", "bc")).times(2).tree();
//        System.out.println(gt.toString());
//        System.out.println(gt.next());
    }

    @Test
    public void testTreeWithBy() {
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
        Tree tree = this.sqlgGraph.traversal().V().hasLabel("A").out().out().tree().by("name").next();
        Assert.assertEquals(1, tree.size());
        Assert.assertTrue(tree.containsKey("a1"));
        Tree tree2  = (Tree) tree.get("a1");
        Assert.assertEquals(1, tree2.size());
        Assert.assertTrue(tree2.containsKey("b1"));
        Tree tree3  = (Tree) tree2.get("b1");
        Assert.assertEquals(3, tree3.size());
        Assert.assertTrue(tree3.containsKey("c1"));
        Assert.assertTrue(tree3.containsKey("c1"));
        Assert.assertTrue(tree3.containsKey("c1"));
    }

    @Test
    public void testTreeWithSideEffect() {
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
        Vertex v = this.sqlgGraph.traversal().V().hasLabel("A").out().out().tree("a").next();
        Assert.assertTrue(Matchers.isOneOf(c1, c2, c3).matches(v));
    }
}
