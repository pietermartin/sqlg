package org.umlg.sqlg.test.mod;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.MutationListener;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.hamcrest.core.Is;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2018/08/26
 */
public class TestTraversalAddV extends BaseTest {

    @Test
    public void g_addVXV_hasXname_markoX_propertiesXnameX_keyX_label() {
        loadModern();
        final Traversal<Vertex, String> traversal =  this.sqlgGraph.traversal().addV(__.V().has("name", "marko").properties("name").key()).label();
        printTraversalForm(traversal);
        Assert.assertEquals("name", traversal.next());
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void g_addV_asXfirstX_repeatXaddEXnextX_toXaddVX_inVX_timesX5X_addEXnextX_toXselectXfirstXX() {
        final Traversal<Vertex, Edge> traversal = this.sqlgGraph.traversal()
                .addV().as("first")
                .repeat(
                        __.addE("next").to(__.addV()).inV()).times(5)
                .addE("next")
                .to(__.select("first"));
        printTraversalForm(traversal);
        Assert.assertEquals("next", traversal.next().label());
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(6L, this.sqlgGraph.traversal().V().count().next().longValue());
        Assert.assertEquals(6L, this.sqlgGraph.traversal().E().count().next().longValue());
        Assert.assertEquals(Arrays.asList(2L, 2L, 2L, 2L, 2L, 2L), this.sqlgGraph.traversal().V().map(__.bothE().count()).toList());
        Assert.assertEquals(Arrays.asList(1L, 1L, 1L, 1L, 1L, 1L), this.sqlgGraph.traversal().V().map(__.inE().count()).toList());
        Assert.assertEquals(Arrays.asList(1L, 1L, 1L, 1L, 1L, 1L), this.sqlgGraph.traversal().V().map(__.outE().count()).toList());
    }

    @Test
    public void shouldDetachVertexWhenAdded() {
        final AtomicBoolean triggered = new AtomicBoolean(false);

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexAdded(final Vertex element) {
                Assert.assertThat(element, IsInstanceOf.instanceOf(DetachedVertex.class));
                Assert.assertEquals("thing", element.label());
                Assert.assertEquals("there", element.value("here"));
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);

        builder.eventQueue(new EventStrategy.TransactionalEventQueue(this.sqlgGraph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.addV("thing").property("here", "there").iterate();
        sqlgGraph.tx().commit();
        Assert.assertThat(triggered.get(), Is.is(true));
    }

    private GraphTraversalSource create(final EventStrategy strategy) {
        return this.sqlgGraph.traversal().withStrategies(strategy);
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
