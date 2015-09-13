package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Created by pieter on 2015/08/30.
 */
public class TestTreeStep extends BaseTest {

    @Test
    public void testTreeStep() {
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
        this.sqlgGraph.traversal().V().hasLabel("A").out().out().tree().next();
        tree = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out()).times(2).tree().next();
        System.out.println(tree);
        GraphTraversal gt = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out("ab", "bc")).times(2).tree();
        System.out.println(gt.toString());
        System.out.println(tree);

    }
}
