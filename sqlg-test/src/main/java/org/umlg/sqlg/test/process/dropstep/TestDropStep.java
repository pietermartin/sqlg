package org.umlg.sqlg.test.process.dropstep;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/11/11
 */
@RunWith(Parameterized.class)
public class TestDropStep extends BaseTest {

    @Parameterized.Parameter
    public Boolean fkOn;

    @Parameterized.Parameters(name = "foreign key cascade: {0}")
    public static Collection<Object[]> data() {
//        return Arrays.asList(new Object[]{Boolean.TRUE}, new Object[]{Boolean.FALSE});
//        return Collections.singletonList(new Object[]{Boolean.FALSE});
        return Collections.singletonList(new Object[]{Boolean.TRUE});
    }

    @Before
    public void before() throws Exception {
        super.before();
        configuration.setProperty("implement.foreign.keys", this.fkOn);
    }

    @Test
    public void testDropStep() {
        this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.traversal().V().hasLabel("A").has("name", "a1").drop().hasNext();
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(a2, a3)));
    }

    @Test
    public void testDropStepWithJoin() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");

        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");

        Edge e1 = a1.addEdge("ab", b1);
        Edge e2 = a1.addEdge("ab", b2);
        Edge e3 = a1.addEdge("ab", b3);

        Edge e4 = b1.addEdge("bc", c1);
        Edge e5 = b2.addEdge("bc", c1);
        Edge e6 = b3.addEdge("bc", c1);

        this.sqlgGraph.tx().commit();

        this.sqlgGraph.traversal()
                .V().hasLabel("A").as("a")
                .out("ab").has("name", "b2")
                .drop().hasNext();
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").out("ab").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(b1, b3)));
        vertices = this.sqlgGraph.traversal().V().hasLabel("B").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(b1, b3)));
        List<Edge> edges = this.sqlgGraph.traversal().E().hasLabel("ab").toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertTrue(edges.containsAll(Arrays.asList(e1, e3)));
    }

//    @Test
//    public void testDropStepWithJoinVertexStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        Edge e1 = a1.addEdge("ab", b1);
//        Edge e2 = a1.addEdge("ab", b2);
//        Edge e3 = a1.addEdge("ab", b3);
//        this.sqlgGraph.tx().commit();
//
//        this.sqlgGraph.traversal()
//                .V().hasLabel("A")
//                .local(
//                        __.out("ab").has("name", "b1").drop()
//                )
//                .hasNext();
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").out("ab").toList();
//        Assert.assertEquals(2, vertices.size());
//        Assert.assertTrue(vertices.containsAll(Arrays.asList(b2, b3)));
//        vertices = this.sqlgGraph.traversal().V().hasLabel("B").toList();
//        Assert.assertEquals(2, vertices.size());
//        Assert.assertTrue(vertices.containsAll(Arrays.asList(b2, b3)));
//        List<Edge> edges = this.sqlgGraph.traversal().E().hasLabel("ab").toList();
//        Assert.assertEquals(2, edges.size());
//        Assert.assertTrue(edges.containsAll(Arrays.asList(e2, e3)));
//    }
//
//    @Test
//    public void testRepeatDrop() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
//        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D");
//        a1.addEdge("ab", b1);
//        b1.addEdge("bc", c1);
//        c1.addEdge("cd", d1);
//        this.sqlgGraph.tx().commit();
//
//        this.sqlgGraph.traversal().V().hasLabel("A")
//                .repeat(__.out())
//                .times(3)
//                .drop()
//                .hasNext();
//        this.sqlgGraph.tx().commit();
//
//        Assert.assertFalse(this.sqlgGraph.traversal().V().hasLabel("D").hasNext());
//        Assert.assertFalse(this.sqlgGraph.traversal().E().hasLabel("cd").hasNext());
//    }

//    @Test
    public void tempOptional() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices =  this.sqlgGraph.traversal().V().hasLabel("A")
                .optional(
                   __.out()
                ).toList();
        System.out.println(vertices);
    }
}
