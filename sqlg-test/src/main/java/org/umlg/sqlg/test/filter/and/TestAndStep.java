package org.umlg.sqlg.test.filter.and;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2017/11/05
 */
public class TestAndStep extends BaseTest {

    @Test
    public void testAndStepOptimized() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "surname", "aa1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "surname", "aa2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "surname", "aa3");
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .and(
                        __.has("name", "a1"),
                        __.has("surname", "aa1")
                );
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(a1));
    }

    @Test
    public void testNestedAndStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "surname", "aa1", "middlename", "aaa1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "surname", "aa2", "middlename", "aaa2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "surname", "aa3", "middlename", "aaa3");
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .and(
                        __.has("name", "a1"),
                        __.and(
                                __.has("surname", "aa1"),
                                __.has("middlename", "aaa1")

                        )
                );
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(a1));

    }
}
