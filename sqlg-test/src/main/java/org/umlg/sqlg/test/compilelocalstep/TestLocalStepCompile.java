package org.umlg.sqlg.test.compilelocalstep;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2016/05/07
 * Time: 1:46 PM
 */
public class TestLocalStepCompile extends BaseTest {

    @Test
    public void testLocalStepCompile() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>)gt.V(a1).local(__.out().out()).path();

        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(1) instanceof LocalStep);
        LocalStep<?,?> localStep = (LocalStep)traversal.getSteps().get(1);
        Assert.assertEquals(1, localStep.getLocalChildren().size());
        Traversal.Admin<?, ?> traversal1 = localStep.getLocalChildren().get(0);
        Assert.assertEquals(2, traversal1.getSteps().size());

        List<Path> paths = traversal.toList();
        Assert.assertEquals(1, paths.size());

        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(1) instanceof LocalStep);
        localStep = (LocalStep)traversal.getSteps().get(1);
        Assert.assertEquals(1, localStep.getLocalChildren().size());
        traversal1 = localStep.getLocalChildren().get(0);
        Assert.assertEquals(1, traversal1.getSteps().size());
    }

    //the limit is executed on the db
    @Test
    public void testLocalStepWithLimitOnDb() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c11 = this.sqlgGraph.addVertex(T.label, "C", "name", "c11");
        Vertex c12 = this.sqlgGraph.addVertex(T.label, "C", "name", "c12");
        Vertex c13 = this.sqlgGraph.addVertex(T.label, "C", "name", "c13");
        Vertex c21 = this.sqlgGraph.addVertex(T.label, "C", "name", "c21");
        Vertex c22 = this.sqlgGraph.addVertex(T.label, "C", "name", "c22");
        Vertex c23 = this.sqlgGraph.addVertex(T.label, "C", "name", "c23");
        Vertex c31 = this.sqlgGraph.addVertex(T.label, "C", "name", "c31");
        Vertex c32 = this.sqlgGraph.addVertex(T.label, "C", "name", "c32");
        Vertex c33 = this.sqlgGraph.addVertex(T.label, "C", "name", "c33");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        b1.addEdge("bc", c11);
        b1.addEdge("bc", c12);
        b1.addEdge("bc", c13);
        b2.addEdge("bc", c21);
        b2.addEdge("bc", c22);
        b2.addEdge("bc", c23);
        b3.addEdge("bc", c31);
        b3.addEdge("bc", c32);
        b3.addEdge("bc", c33);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1)
                .local(
                        __.out().limit(1).out()
                );
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(3, vertices.size());

    }

    //the limit is NOT executed on the db
    @Test
    public void testLocalStepWithLimitNotOnDb() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex bb1 = this.sqlgGraph.addVertex(T.label, "BB", "name", "bb1");
        Vertex bb2 = this.sqlgGraph.addVertex(T.label, "BB", "name", "bb2");
        Vertex bb3 = this.sqlgGraph.addVertex(T.label, "BB", "name", "bb3");
        Vertex c11 = this.sqlgGraph.addVertex(T.label, "C", "name", "c11");
        Vertex c12 = this.sqlgGraph.addVertex(T.label, "C", "name", "c12");
        Vertex c13 = this.sqlgGraph.addVertex(T.label, "C", "name", "c13");
        Vertex c21 = this.sqlgGraph.addVertex(T.label, "C", "name", "c21");
        Vertex c22 = this.sqlgGraph.addVertex(T.label, "C", "name", "c22");
        Vertex c23 = this.sqlgGraph.addVertex(T.label, "C", "name", "c23");
        Vertex c31 = this.sqlgGraph.addVertex(T.label, "C", "name", "c31");
        Vertex c32 = this.sqlgGraph.addVertex(T.label, "C", "name", "c32");
        Vertex c33 = this.sqlgGraph.addVertex(T.label, "C", "name", "c33");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        a1.addEdge("abb", bb1);
        a1.addEdge("abb", bb2);
        a1.addEdge("abb", bb3);

        b1.addEdge("bc", c11);
        b1.addEdge("bc", c12);
        b1.addEdge("bc", c13);
        b2.addEdge("bc", c21);
        b2.addEdge("bc", c22);
        b2.addEdge("bc", c23);
        b3.addEdge("bc", c31);
        b3.addEdge("bc", c32);
        b3.addEdge("bc", c33);

        bb1.addEdge("bc", c11);
        bb1.addEdge("bc", c12);
        bb1.addEdge("bc", c13);
        bb2.addEdge("bc", c21);
        bb2.addEdge("bc", c22);
        bb2.addEdge("bc", c23);
        bb3.addEdge("bc", c31);
        bb3.addEdge("bc", c32);
        bb3.addEdge("bc", c33);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1)
//                .out().limit(1).out();
                .local(
                        __.out().limit(1).out()
                );
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(3, vertices.size());
    }

}
