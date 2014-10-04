package org.umlg.sqlg.test.mod;

import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.strategy.*;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.junit.Assert.*;

/**
 * Date: 2014/08/28
 * Time: 7:14 AM
 */
public class TestUpdateVertex extends BaseTest {

    @Test
    public void testUpdateVertex() {
        Vertex v = this.sqlG.addVertex(T.label, "Person", "name", "john");
        Assert.assertEquals("john", v.value("name"));
        v.property("name", "joe");
        Assert.assertEquals("joe", v.value("name"));
        this.sqlG.tx().commit();
        Assert.assertEquals("joe", v.value("name"));
    }

    @Test
    public void testPropertyIsPresent() {
        Vertex v = this.sqlG.addVertex(T.label, "Person", "name", "john");
        Assert.assertTrue(v.property("name").isPresent());
    }

    @Test
    public void shouldNotCallBaseFunctionThusNotRemovingTheVertex() throws Exception {
        Graph g = this.sqlG;
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);

        // create an ad-hoc strategy that only marks a vertex as "deleted" and removes all edges and properties
        // but doesn't actually blow it away
        swg.strategy().setGraphStrategy(new GraphStrategy() {
            @Override
            public UnaryOperator<Supplier<Void>> getRemoveVertexStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
                return (t) -> () -> {
                    final Vertex v = ctx.getCurrent().getBaseVertex();
                    v.bothE().remove();
                    v.properties().forEachRemaining(Property::remove);
                    v.property("deleted", true);
                    return null;
                };
            }
        });

        final Vertex toRemove = g.addVertex("name", "pieter");
        toRemove.addEdge("likes", g.addVertex("feature", "Strategy"));

        assertEquals(1, toRemove.properties().count().next(), 0);
        assertEquals(new Long(1), toRemove.bothE().count().next());
        assertFalse(toRemove.property("deleted").isPresent());

        swg.v(toRemove.id()).remove();

        final Vertex removed = g.v(toRemove.id());
        assertNotNull(removed);
        assertEquals(1, removed.properties().count().next(), 0);
        assertEquals(new Long(0), removed.bothE().count().next());
        assertTrue(toRemove.property("deleted").isPresent());
    }

}
