package org.umlg.sqlg.test.process.dropstep;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.MutationListener;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/11/11
 */
@RunWith(Parameterized.class)
public class TestDropStep extends BaseTest {

    @Parameterized.Parameter
    public Boolean fkOn;
    @Parameterized.Parameter(1)
    public Boolean mutatingCallback;
    private List<Vertex> removedVertices = new ArrayList<>();
    private List<Edge> removedEdges = new ArrayList<>();
    private EventStrategy eventStrategy = null;
    private GraphTraversalSource dropTraversal;

    @Parameterized.Parameters(name = "foreign key implement foreign keys: {0}, callback {1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[]{Boolean.TRUE, Boolean.FALSE}, new Object[]{Boolean.FALSE, Boolean.FALSE},
                new Object[]{Boolean.TRUE, Boolean.TRUE}, new Object[]{Boolean.FALSE, Boolean.TRUE});
//        return Collections.singletonList(new Object[]{Boolean.FALSE, Boolean.FALSE});
//        return Collections.singletonList(new Object[]{Boolean.TRUE, Boolean.TRUE});
    }

    @Before
    public void before() throws Exception {
        super.before();
        configuration.setProperty("implement.foreign.keys", this.fkOn);
        this.removedVertices.clear();
        if (this.mutatingCallback) {
//            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportReturningDeletedRows());
            final MutationListener listener = new AbstractMutationListener() {
                @Override
                public void vertexRemoved(final Vertex vertex) {
                    removedVertices.add(vertex);
                }

                @Override
                public void edgeRemoved(final Edge edge) {
                    removedEdges.add(edge);
                }
            };
            final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);
            eventStrategy = builder.create();
            this.dropTraversal = this.sqlgGraph.traversal();
            if (this.mutatingCallback) {
                this.dropTraversal = this.dropTraversal.withStrategies(this.eventStrategy);
            }
        } else {
            this.dropTraversal = this.sqlgGraph.traversal();
        }
    }

    @Test
    public void shouldReferenceVertexWhenRemoved() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = this.sqlgGraph.addVertex();
        final String label = v.label();
        final Object id = v.id();

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexRemoved(final Vertex element) {
                Assert.assertThat(element, IsInstanceOf.instanceOf(ReferenceVertex.class));
                Assert.assertEquals(id, element.id());
                Assert.assertEquals(label, element.label());
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener).detach(ReferenceFactory.class);

        if (this.sqlgGraph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(this.sqlgGraph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = this.sqlgGraph.traversal().withStrategies(eventStrategy);

        gts.V(v).drop().iterate();
        this.sqlgGraph.tx().commit();

        AbstractGremlinTest.assertVertexEdgeCounts(this.sqlgGraph, 0, 0);
        Assert.assertThat(triggered.get(), CoreMatchers.is(true));
    }

    @Test
    public void testDropStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        this.sqlgGraph.tx().commit();

        this.dropTraversal.V().hasLabel("A").has("name", "a1").drop().hasNext();
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(a2, a3)));
        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertTrue(this.removedVertices.contains(a1));
        }
    }

    @Test
    public void testDropStepRepeat1() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        c1.addEdge("cd", d1);
        this.sqlgGraph.tx().commit();

        this.dropTraversal.V().hasLabel("A")
                .repeat(__.out())
                .times(3)
                .drop()
                .hasNext();
        this.sqlgGraph.tx().commit();

        Assert.assertFalse(this.sqlgGraph.traversal().V().hasLabel("D").hasNext());
        Assert.assertFalse(this.sqlgGraph.traversal().E().hasLabel("cd").hasNext());
        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertEquals(1, this.removedEdges.size());
        }
    }

    @Test
    public void testDropStepRepeat2() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        c1.addEdge("cd", d1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D");
        b2.addEdge("ab", a2);
        c2.addEdge("bc", b2);
        d2.addEdge("cd", c2);
        this.sqlgGraph.tx().commit();

        this.dropTraversal.V().hasLabel("A")
                .repeat(__.out()).times(3)
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("D").count().next(), 0);

        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertEquals(1, this.removedEdges.size());
        }

        this.dropTraversal.V().hasLabel("A")
                .repeat(__.in()).times(3)
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("D").count().next(), 0);
        if (this.mutatingCallback) {
            Assert.assertEquals(2, this.removedVertices.size());
            Assert.assertEquals(2, this.removedEdges.size());
        }
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

        this.dropTraversal
                .V().hasLabel("A").as("a")
                .out("ab").has("name", "b2")
                .drop().iterate();
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

        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertTrue(this.removedVertices.contains(b2));
            Assert.assertTrue(this.removedEdges.contains(e2));
        }
    }

    @Test
    public void testDropEdges() {
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

        this.dropTraversal
                .V().hasLabel("A").as("a")
                .outE("ab")
                .drop().hasNext();
        this.sqlgGraph.tx().commit();

        Assert.assertFalse(this.sqlgGraph.traversal().E().hasLabel("ab").hasNext());
        Assert.assertEquals(3L, this.sqlgGraph.traversal().E().hasLabel("bc").count().next(), 0L);
        if (this.mutatingCallback) {
            Assert.assertEquals(3, this.removedEdges.size());
            Assert.assertEquals(0, this.removedVertices.size());
            Assert.assertTrue(this.removedEdges.containsAll(Arrays.asList(e1, e2, e3)));
        }

    }

    @Test
    public void dropAll() {
        loadModern(this.sqlgGraph);
        this.dropTraversal.V().drop().iterate();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.dropTraversal.V().hasNext());
        Assert.assertEquals(0, IteratorUtils.count(this.sqlgGraph.traversal().V()));
        Assert.assertEquals(0, IteratorUtils.count(this.sqlgGraph.traversal().E()));
        if (this.mutatingCallback) {
            Assert.assertEquals(6, this.removedVertices.size());
            Assert.assertEquals(6, this.removedEdges.size());
        }
    }

    @Test
    public void dropAllEdges() {
        loadModern(this.sqlgGraph);
        this.dropTraversal.E().drop().iterate();
        Assert.assertFalse(this.dropTraversal.E().hasNext());
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(6, IteratorUtils.count(this.sqlgGraph.traversal().V()));
        Assert.assertEquals(0, IteratorUtils.count(this.sqlgGraph.traversal().E()));
        if (this.mutatingCallback) {
            Assert.assertEquals(0, this.removedVertices.size());
            Assert.assertEquals(6, this.removedEdges.size());
        }
    }

    @Test
    public void dropMultiplePathsToVertices() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
        Vertex c5 = this.sqlgGraph.addVertex(T.label, "C", "name", "c5");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        b2.addEdge("bc", c3);
        b2.addEdge("bc", c4);
        b2.addEdge("bc", c5);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A")
                .out()
                .out()
                .toList();
        Assert.assertEquals(6, vertices.size());
        Assert.assertTrue(vertices.removeAll(Arrays.asList(c1, c2, c3, c4, c5)));
        Assert.assertEquals(0, vertices.size());

        this.dropTraversal.V().hasLabel("A")
                .out()
                .out()
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(0L, this.sqlgGraph.traversal().V().hasLabel("C").count().next(), 0L);
        Assert.assertEquals(0L, this.sqlgGraph.traversal().E().hasLabel("bc").count().next(), 0L);

        if (this.mutatingCallback) {
            Assert.assertEquals(5, this.removedVertices.size());
            Assert.assertEquals(6, this.removedEdges.size());
        }

    }

    @Test
    public void dropMultiplePathsToVerticesWithHas() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
        Vertex c5 = this.sqlgGraph.addVertex(T.label, "C", "name", "c5");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        b2.addEdge("bc", c3);
        b2.addEdge("bc", c4);
        b2.addEdge("bc", c5);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A")
                .out()
                .out().has("name", P.within("c1", "c2", "c3", "c4"))
                .toList();
        Assert.assertEquals(5, vertices.size());
        Assert.assertTrue(vertices.removeAll(Arrays.asList(c1, c2, c3, c4)));
        Assert.assertEquals(0, vertices.size());

        this.dropTraversal.V().hasLabel("A")
                .out()
                .out().has("name", P.within("c1", "c2", "c3", "c4"))
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1L, this.sqlgGraph.traversal().V().hasLabel("C").count().next(), 0L);
        Assert.assertEquals(1L, this.sqlgGraph.traversal().E().hasLabel("bc").count().next(), 0L);

        if (this.mutatingCallback) {
            Assert.assertEquals(4, this.removedVertices.size());
            Assert.assertEquals(5, this.removedEdges.size());
        }
    }

    @Test
    public void testDropStepWithJoinVertexStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Edge e1 = a1.addEdge("ab", b1);
        Edge e2 = a1.addEdge("ab", b2);
        Edge e3 = a1.addEdge("ab", b3);
        this.sqlgGraph.tx().commit();

        this.dropTraversal
                .V().hasLabel("A")
                .local(
                        __.out("ab").has("name", "b2").drop()
                )
                .iterate();
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
        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertEquals(1, this.removedEdges.size());
        }
    }

    static abstract class AbstractMutationListener implements MutationListener {
        @Override
        public void vertexAdded(final Vertex vertex) {

        }

        @Override
        public void vertexRemoved(final Vertex vertex) {

        }

        @Override
        public void vertexPropertyChanged(final Vertex element, final Property oldValue, final Object setValue, final Object... vertexPropertyKeyValues) {

        }

        @Override
        public void vertexPropertyRemoved(final VertexProperty vertexProperty) {

        }

        @Override
        public void edgeAdded(final Edge edge) {

        }

        @Override
        public void edgeRemoved(final Edge edge) {

        }

        @Override
        public void edgePropertyChanged(final Edge element, final Property oldValue, final Object setValue) {

        }

        @Override
        public void edgePropertyRemoved(final Edge element, final Property property) {

        }

        @Override
        public void vertexPropertyPropertyChanged(final VertexProperty element, final Property oldValue, final Object setValue) {

        }

        @Override
        public void vertexPropertyPropertyRemoved(final VertexProperty element, final Property property) {

        }
    }
}
