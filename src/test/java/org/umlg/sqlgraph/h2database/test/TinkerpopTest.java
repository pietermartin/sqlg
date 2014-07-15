package org.umlg.sqlgraph.h2database.test;

import com.tinkerpop.gremlin.GraphProvider;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import com.tinkerpop.gremlin.structure.strategy.*;
import junit.framework.Assert;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.umlg.sqlgraph.structure.SqlGraph;
import org.umlg.tinkerpop3.test.SqlGraphProvider;

import java.io.*;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static com.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_INTEGER_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_STRING_VALUES;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2014/07/13
 * Time: 6:32 PM
 */
public class TinkerpopTest extends BaseTest {

    @Test
    public void shouldNotCallBaseFunctionThusNotRemovingTheVertex() throws Exception {
        Graph g = this.sqlGraph;
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);

        // create an ad-hoc strategy that only marks a vertex as "deleted" and removes all edges and properties
        // but doesn't actually blow it away
        swg.strategy().setGraphStrategy(new GraphStrategy() {
            @Override
            public UnaryOperator<Supplier<Void>> getRemoveElementStrategy(final Strategy.Context<? extends StrategyWrappedElement> ctx) {
                if (ctx.getCurrent() instanceof StrategyWrappedVertex) {
                    return (t) -> () -> {
                        final Vertex v = ((StrategyWrappedVertex) ctx.getCurrent()).getBaseVertex();
                        v.bothE().remove();
                        v.properties().values().forEach(Property::remove);
                        v.property("deleted", true);
                        return null;
                    };
                } else {
                    return UnaryOperator.identity();
                }
            }
        });

        final Vertex toRemove = g.addVertex("name", "pieter");
        toRemove.addEdge("likes", g.addVertex("feature", "Strategy"));

        assertEquals(1, toRemove.properties().size());
        assertEquals(new Long(1), toRemove.bothE().count().next());
        assertFalse(toRemove.property("deleted").isPresent());

        swg.v(toRemove.id()).remove();

        final Vertex removed = g.v(toRemove.id());
        assertNotNull(removed);
        assertEquals(1, removed.properties().size());
        assertEquals(new Long(0), removed.bothE().count().next());
        assertTrue(toRemove.property("deleted").isPresent());
    }

    @Test
    public void shouldReadWriteClassicToGraphSON() throws Exception {
        readGraphMLIntoGraph(this.sqlGraph);
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.create().build();
            writer.writeGraph(os, this.sqlGraph);

            GraphProvider graphProvider = new SqlGraphProvider();
            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph");
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final GraphSONReader reader = GraphSONReader.create().build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            assertClassicGraph(g1, true, false);

            // need to manually close the "g1" instance
            graphProvider.clear(g1, configuration);
        }
    }

    @Test
    public void shouldReadGraphML() throws IOException {
        readGraphMLIntoGraph(this.sqlGraph);
        assertClassicGraph(this.sqlGraph, false, true);
    }

    private static void readGraphMLIntoGraph(final Graph g) throws IOException {
        final GraphReader reader = GraphMLReader.create().build();
        try (final InputStream stream = new FileInputStream(new File("src/test/resources/tinkerpop-classic.xml"))) {
            reader.readGraph(stream, g);
        }
//        try (final InputStream stream = TinkerpopTest.class.getResourceAsStream("tinkerpop-classic.xml")) {
//            reader.readGraph(stream, g);
//        }
    }

    public static void assertClassicGraph(final Graph g1, final boolean lossyForFloat, final boolean lossyForId) {
        assertEquals(new Long(6), g1.V().count().next());
        assertEquals(new Long(6), g1.E().count().next());

        final Vertex v1 = (Vertex) g1.V().has("name", "marko").next();
        assertEquals(29, v1.<Integer>value("age").intValue());
        assertEquals(2, v1.keys().size());
        assertClassicId(g1, lossyForId, v1, 1);

        final List<Edge> v1Edges = v1.bothE().toList();
        assertEquals(3, v1Edges.size());
        v1Edges.forEach(e -> {
            if (e.inV().value("name").next().equals("vadas")) {
                assertEquals("knows", e.label());
                if (lossyForFloat)
                    assertEquals(0.5d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.5f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertClassicId(g1, lossyForId, e, 7);
            } else if (e.inV().value("name").next().equals("josh")) {
                assertEquals("knows", e.label());
                if (lossyForFloat)
                    assertEquals(1.0, e.value("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertClassicId(g1, lossyForId, e, 8);
            } else if (e.inV().value("name").next().equals("lop")) {
                assertEquals("created", e.label());
                if (lossyForFloat)
                    assertEquals(0.4d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertClassicId(g1, lossyForId, e, 9);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v2 = (Vertex) g1.V().has("name", "vadas").next();
        assertEquals(27, v2.<Integer>value("age").intValue());
        assertEquals(2, v2.keys().size());
        assertClassicId(g1, lossyForId, v2, 2);

        final List<Edge> v2Edges = v2.bothE().toList();
        assertEquals(1, v2Edges.size());
        v2Edges.forEach(e -> {
            if (e.outV().value("name").next().equals("marko")) {
                assertEquals("knows", e.label());
                if (lossyForFloat)
                    assertEquals(0.5d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.5f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertClassicId(g1, lossyForId, e, 7);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v3 = (Vertex) g1.V().has("name", "lop").next();
        assertEquals("java", v3.<String>value("lang"));
        assertEquals(2, v2.keys().size());
        assertClassicId(g1, lossyForId, v3, 3);

        final List<Edge> v3Edges = v3.bothE().toList();
        assertEquals(3, v3Edges.size());
        v3Edges.forEach(e -> {
            if (e.outV().value("name").next().equals("peter")) {
                assertEquals("created", e.label());
                if (lossyForFloat)
                    assertEquals(0.2d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.2f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertClassicId(g1, lossyForId, e, 12);
            } else if (e.outV().next().value("name").equals("josh")) {
                assertEquals("created", e.label());
                if (lossyForFloat)
                    assertEquals(0.4d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertClassicId(g1, lossyForId, e, 11);
            } else if (e.outV().value("name").next().equals("marko")) {
                assertEquals("created", e.label());
                if (lossyForFloat)
                    assertEquals(0.4d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertClassicId(g1, lossyForId, e, 9);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v4 = (Vertex) g1.V().has("name", "josh").next();
        assertEquals(32, v4.<Integer>value("age").intValue());
        assertEquals(2, v4.keys().size());
        assertClassicId(g1, lossyForId, v4, 4);

        final List<Edge> v4Edges = v4.bothE().toList();
        assertEquals(3, v4Edges.size());
        v4Edges.forEach(e -> {
            if (e.inV().value("name").next().equals("ripple")) {
                assertEquals("created", e.label());
                if (lossyForFloat)
                    assertEquals(1.0d, e.value("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertClassicId(g1, lossyForId, e, 10);
            } else if (e.inV().value("name").next().equals("lop")) {
                assertEquals("created", e.label());
                if (lossyForFloat)
                    assertEquals(0.4d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertClassicId(g1, lossyForId, e, 11);
            } else if (e.outV().value("name").next().equals("marko")) {
                assertEquals("knows", e.label());
                if (lossyForFloat)
                    assertEquals(1.0d, e.value("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertClassicId(g1, lossyForId, e, 8);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v5 = (Vertex) g1.V().has("name", "ripple").next();
        assertEquals("java", v5.<String>value("lang"));
        assertEquals(2, v5.keys().size());
        assertClassicId(g1, lossyForId, v5, 5);

        final List<Edge> v5Edges = v5.bothE().toList();
        assertEquals(1, v5Edges.size());
        v5Edges.forEach(e -> {
            if (e.outV().value("name").next().equals("josh")) {
                assertEquals("created", e.label());
                if (lossyForFloat)
                    assertEquals(1.0d, e.value("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertClassicId(g1, lossyForId, e, 10);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v6 = (Vertex) g1.V().has("name", "peter").next();
        assertEquals(35, v6.<Integer>value("age").intValue());
        assertEquals(2, v6.keys().size());
        assertClassicId(g1, lossyForId, v6, 6);

        final List<Edge> v6Edges = v6.bothE().toList();
        assertEquals(1, v6Edges.size());
        v6Edges.forEach(e -> {
            if (e.inV().value("name").next().equals("lop")) {
                assertEquals("created", e.label());
                if (lossyForFloat)
                    assertEquals(0.2d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.2f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertClassicId(g1, lossyForId, e, 12);
            } else {
                fail("Edge not expected");
            }
        });
    }

    private static void assertClassicId(final Graph g, final boolean lossyForId, final Element e, final Object expected) {
        if (g.getFeatures().edge().supportsUserSuppliedIds()) {
            if (lossyForId)
                assertEquals(expected.toString(), e.id().toString());
            else
                assertEquals(expected, e.id());
        }
    }

    protected void tryCommit(final SqlGraph g, final Consumer<Graph> assertFunction) {
        assertFunction.accept(g);
        if (g.getFeatures().graph().supportsTransactions()) {
            g.tx().commit();
            assertFunction.accept(g);
        }
    }

    protected void tryCommit(final Graph g) {
        if (g.getFeatures().graph().supportsTransactions())
            g.tx().commit();
    }

    protected void tryRollback(final Graph g) {
        if (g.getFeatures().graph().supportsTransactions())
            g.tx().rollback();
    }

}
