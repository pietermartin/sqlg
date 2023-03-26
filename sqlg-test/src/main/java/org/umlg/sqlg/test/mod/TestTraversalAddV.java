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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2018/08/26
 */
public class TestTraversalAddV extends BaseTest {

//    Scenario: g_V_addVXanimalX_propertyXage_0X
//    Given the empty graph
//    And the graph initializer of
//      """
//      g.addV("person").property("name", "marko").property("age", 29).as("marko").
//        addV("person").property("name", "vadas").property("age", 27).as("vadas").
//        addV("software").property("name", "lop").property("lang", "java").as("lop").
//        addV("person").property("name","josh").property("age", 32).as("josh").
//        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
//        addV("person").property("name", "peter").property("age", 35).as('peter').
//        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
//        addE("knows").from("marko").to("josh").property("weight", 1.0d).
//        addE("created").from("marko").to("lop").property("weight", 0.4d).
//        addE("created").from("josh").to("ripple").property("weight", 1.0d).
//        addE("created").from("josh").to("lop").property("weight", 0.4d).
//        addE("created").from("peter").to("lop").property("weight", 0.2d)
//      """
//    And the traversal of
//      """
//      g.V().addV("animal").property("age", 0)
//      """
//    When iterated to list
//    Then the result should have a count of 6
//    And the graph should return 6 for count of "g.V().has(\"animal\",\"age\",0)"
    @Test
    public void g_V_addVXanimalX_propertyXage_0X() {
      this.sqlgGraph.traversal().addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as("peter").
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d).iterate();

        List<Vertex> vertices =  this.sqlgGraph.traversal().V().addV("animal").property("age", 0).toList();
        Assert.assertEquals(6, vertices.size());

    }

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
