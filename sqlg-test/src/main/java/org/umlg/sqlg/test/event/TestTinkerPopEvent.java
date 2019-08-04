package org.umlg.sqlg.test.event;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.MutationListener;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/06/13
 */
public class TestTinkerPopEvent extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
            configuration.setProperty("cache.vertices", true);
            if (!configuration.containsKey("jdbc.url")) {
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
            }
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "John");
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1).next();
        Vertex a1Again = this.sqlgGraph.traversal().V(a1).next();
        a1Again.property("name", "Peter");
        this.sqlgGraph.tx().commit();

        //This fails, TinkerPop does not specify transaction memory visibility
        Assert.assertEquals("Peter", a1.value("name"));
    }

    @Test
    public void shouldDetachVertexPropertyWhenRemoved() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = this.sqlgGraph.addVertex();
        final VertexProperty vp = v.property("to-remove","blah");
        final String label = vp.label();
        final Object value = vp.value();
        final VertexProperty vpToKeep = v.property("to-keep","dah");

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexPropertyRemoved(final VertexProperty element) {
                Assert.assertEquals(label, element.label());
                Assert.assertEquals(value, element.value());
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);

        if (this.sqlgGraph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(this.sqlgGraph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.V(v).properties("to-remove").drop().iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, IteratorUtils.count(v.properties()));
        Assert.assertEquals(vpToKeep.value(), v.value("to-keep"));
        Assert.assertThat(triggered.get(), CoreMatchers.is(true));
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
