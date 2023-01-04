package org.umlg.sqlg.test.process.dropstep;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.MutationListener;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.*;
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

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2017/11/23
 */
@RunWith(Parameterized.class)
public class TestDropStepTruncate extends BaseTest {

    @Parameterized.Parameter
    public Boolean fkOn;
    @Parameterized.Parameter(1)
    public Boolean mutatingCallback;

    private final List<Vertex> removedVertices = new ArrayList<>();
    private final List<Edge> removedEdges = new ArrayList<>();
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
            EventStrategy eventStrategy = builder.create();
            this.dropTraversal = this.sqlgGraph.traversal();
            if (this.mutatingCallback) {
                this.dropTraversal = this.dropTraversal.withStrategies(eventStrategy);
            }
        } else {
            this.dropTraversal = this.sqlgGraph.traversal();
        }
    }

    @Test
    public void testTruncateVertexOnly() {
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "B");
        this.sqlgGraph.tx().commit();
        this.dropTraversal.V().drop().iterate();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.traversal().V().hasNext());
        if (this.mutatingCallback) {
            Assert.assertEquals(2, this.removedVertices.size());
            Assert.assertEquals(0, this.removedEdges.size());
        }
    }

    @Test
    public void testTruncateEdgeOnly() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        a1.addEdge("abb", b1);
        this.sqlgGraph.tx().commit();
        this.dropTraversal.E().drop().iterate();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.traversal().E().hasNext());
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().count().next(), 0);
    }

    @Test
    public void testTruncateVertexWithOnlyOneEdge() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.traversal().V().hasLabel("A").drop().iterate();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.traversal().E().hasNext());
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().count().next(), 0);
    }

    /**
     * This does not call TRUNCATE. Truncate is not possible when there are multiple foreign keys.
     */
    @Test
    public void testTruncateVertexLabelWithMultipleOutEdgeLabels() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        a1.addEdge("ab", b1);
        c1.addEdge("ab", a1);
        this.sqlgGraph.tx().commit();
        this.dropTraversal.V().hasLabel("A").drop().iterate();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.traversal().E().hasNext());
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().count().next(), 0);

    }

    static abstract class AbstractMutationListener implements MutationListener {
        @Override
        public void vertexAdded(final Vertex vertex) {

        }

        @Override
        public void vertexRemoved(final Vertex vertex) {

        }

        @Override
        public void vertexPropertyChanged(Vertex element, VertexProperty oldValue, Object setValue, Object... vertexPropertyKeyValues) {

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
