package org.umlg.sqlg.test.gremlincompile;

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
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");

        Vertex b11 = this.sqlgGraph.addVertex(T.label, "B", "name", "b11");
        Vertex b12 = this.sqlgGraph.addVertex(T.label, "B", "name", "b12");
        Vertex b13 = this.sqlgGraph.addVertex(T.label, "B", "name", "b13");
        a1.addEdge("ab", b11);
        a1.addEdge("ab", b12);
        a1.addEdge("ab", b13);

        Vertex b21 = this.sqlgGraph.addVertex(T.label, "B", "name", "b21");
        Vertex b22 = this.sqlgGraph.addVertex(T.label, "B", "name", "b22");
        Vertex b23 = this.sqlgGraph.addVertex(T.label, "B", "name", "b23");
        a2.addEdge("ab", b21);
        a2.addEdge("ab", b22);
        a2.addEdge("ab", b23);

        Vertex b31 = this.sqlgGraph.addVertex(T.label, "B", "name", "b31");
        Vertex b32 = this.sqlgGraph.addVertex(T.label, "B", "name", "b32");
        Vertex b33 = this.sqlgGraph.addVertex(T.label, "B", "name", "b33");
        a3.addEdge("ab", b31);
        a3.addEdge("ab", b32);
        a3.addEdge("ab", b33);

        this.sqlgGraph.tx().commit();

        Tree tree = this.sqlgGraph.traversal().V().hasLabel("A").out().tree().next();
        System.out.println(tree);


    }
}
