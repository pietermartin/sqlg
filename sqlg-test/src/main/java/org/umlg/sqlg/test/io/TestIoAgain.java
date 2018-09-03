package org.umlg.sqlg.test.io;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.umlg.sqlg.test.BaseTest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.junit.Assert.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/06/24
 */
@RunWith(Parameterized.class)
public class TestIoAgain extends BaseTest  {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"graphson-v1", false, false,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V1_0)).reader().create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V1_0)).writer().create()},
                {"graphson-v1-embedded", true, false,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V1_0)).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V1_0)).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V1_0)).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V1_0)).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create()).create()},
                {"graphson-v2", false, false,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V2_0)).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.NO_TYPES).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V2_0)).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.NO_TYPES).create()).create()},
                {"graphson-v2-embedded", true, false,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V2_0)).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V2_0)).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create()).create()},
                {"graphson-v3", true, false,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V3_0)).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V3_0)).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().create()).create()},
                {"gryo-v1", true, true,
                        (Function<Graph, GraphReader>) g -> g.io(GryoIo.build(GryoVersion.V1_0)).reader().create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GryoIo.build(GryoVersion.V1_0)).writer().create()},
                {"gryo-v3", true, true,
                        (Function<Graph, GraphReader>) g -> g.io(GryoIo.build(GryoVersion.V3_0)).reader().create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GryoIo.build(GryoVersion.V3_0)).writer().create()}
        });
    }

    @Parameterized.Parameter()
    public String ioType;

    @Parameterized.Parameter(value = 1)
    public boolean assertViaDirectEquality;

    @Parameterized.Parameter(value = 2)
    public boolean assertEdgesAtSameTimeAsVertex;

    @Parameterized.Parameter(value = 3)
    public Function<Graph, GraphReader> readerMaker;

    @Parameterized.Parameter(value = 4)
    public Function<Graph, GraphWriter> writerMaker;

//    @Test
    public void shouldReadWriteVertexWithBOTHEdges() throws Exception {
        Graph graph = this.sqlgGraph;
        final Vertex v1 = graph.addVertex("name", "marko", T.label, "person");

        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5d);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphWriter writer = writerMaker.apply(graph);
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge1 = new AtomicBoolean(false);
            final AtomicBoolean calledEdge2 = new AtomicBoolean(false);

            final GraphReader reader = readerMaker.apply(graph);
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, attachable -> {
                    final Vertex detachedVertex = attachable.get();
                    if (assertViaDirectEquality) {
                        TestHelper.validateVertexEquality(v1, detachedVertex, assertEdgesAtSameTimeAsVertex);
                    } else {
                        assertEquals(v1.id(), graph.vertices(detachedVertex.id().toString()).next().id());
                        assertEquals(v1.label(), detachedVertex.label());
                        assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                        assertEquals("marko", detachedVertex.value("name"));
                    }
                    calledVertex.set(true);
                    return detachedVertex;
                }, attachable -> {
                    final Edge detachedEdge = attachable.get();
                    final Predicate<Edge> matcher = assertViaDirectEquality ? e -> detachedEdge.id().equals(e.id()) :
                            e -> graph.edges(detachedEdge.id().toString()).next().id().equals(e.id());
                    if (matcher.test(e1)) {
                        if (assertViaDirectEquality) {
                            TestHelper.validateEdgeEquality(e1, detachedEdge);
                        } else {
                            assertEquals(e1.id(), graph.edges(detachedEdge.id().toString()).next().id());
                            assertEquals(v1.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                            assertEquals(v2.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                            assertEquals(v2.label(), detachedEdge.inVertex().label());
                            assertEquals(e1.label(), detachedEdge.label());
                            assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                            assertEquals(0.5d, detachedEdge.value("weight"), 0.000001d);
                        }
                        calledEdge1.set(true);
                    } else if (matcher.test(e2)) {
                        if (assertViaDirectEquality) {
                            TestHelper.validateEdgeEquality(e2, detachedEdge);
                        } else {
                            assertEquals(e2.id(), graph.edges(detachedEdge.id().toString()).next().id());
                            assertEquals(v2.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                            assertEquals(v1.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                            assertEquals(v1.label(), detachedEdge.outVertex().label());
                            assertEquals(e2.label(), detachedEdge.label());
                            assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                            assertEquals(1.0d, detachedEdge.value("weight"), 0.000001d);
                        }
                        calledEdge2.set(true);
                    } else {
                        fail("An edge id generated that does not exist");
                    }

                    return null;
                }, Direction.BOTH);
            }

            assertTrue(calledVertex.get());
            assertTrue(calledEdge1.get());
            assertTrue(calledEdge2.get());
        }
    }

    private final org.apache.tinkerpop.shaded.jackson.core.type.TypeReference<HashMap<String, Object>> mapTypeReference = new org.apache.tinkerpop.shaded.jackson.core.type.TypeReference<HashMap<String, Object>>() {
    };

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldSerializeTree() throws Exception {
        loadModern();
        final org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper mapper = this.sqlgGraph.io(GraphSONIo.build(GraphSONVersion.V1_0)).mapper().version(GraphSONVersion.V1_0).create().createMapper();
        Traversal<Vertex, Tree> traversal = this.sqlgGraph.traversal().V(convertToVertexId("marko")).out().properties("name").tree();
        printTraversalForm(traversal);
        Tree t = traversal.next();
        final String json = mapper.writeValueAsString(t);

        final HashMap<String, Object> m = (HashMap<String, Object>) mapper.readValue(json, mapTypeReference);

        // Check Structure
        assertEquals(1, m.size());
        assertTrue(m.containsKey(convertToVertexId("marko").toString()));

        // Check Structure n+1
        final HashMap<String, Object> branch = (HashMap<String, Object>) m.get(convertToVertexId("marko").toString());
        assertEquals(2, branch.size());
        assertTrue(branch.containsKey(GraphSONTokens.KEY));
        assertTrue(branch.containsKey(GraphSONTokens.VALUE));

        //Check n+1 key (traversed element)
        final HashMap<String, Object> branchKey = (HashMap<String, Object>) branch.get(GraphSONTokens.KEY);
        assertTrue(branchKey.containsKey(GraphSONTokens.ID));
        assertTrue(branchKey.containsKey(GraphSONTokens.LABEL));
        assertTrue(branchKey.containsKey(GraphSONTokens.TYPE));
        assertTrue(branchKey.containsKey(GraphSONTokens.PROPERTIES));
        assertEquals(convertToVertexId("marko").toString(), branchKey.get(GraphSONTokens.ID).toString());
        assertEquals("person", branchKey.get(GraphSONTokens.LABEL));
        assertEquals("vertex", branchKey.get(GraphSONTokens.TYPE));
        final HashMap<String, List<HashMap<String, Object>>> branchKeyProps = (HashMap<String, List<HashMap<String, Object>>>) branchKey.get(GraphSONTokens.PROPERTIES);
        assertEquals("marko", branchKeyProps.get("name").get(0).get("value"));
        assertEquals(29, branchKeyProps.get("age").get(0).get("value"));

        //Check n+1 value (traversed element)
        final HashMap<String, Object> branchValue = (HashMap<String, Object>) branch.get(GraphSONTokens.VALUE);
        assertEquals(3, branchValue.size());
        assertTrue(branchValue.containsKey(convertToVertexId("vadas").toString()));
        assertTrue(branchValue.containsKey(convertToVertexId("lop").toString()));
        assertTrue(branchValue.containsKey(convertToVertexId("josh").toString()));

        // Check that vp[] functioned properly
        final HashMap<String, HashMap<String, Object>> branch2 = (HashMap<String, HashMap<String, Object>>) branchValue.get(convertToVertexId("vadas").toString());
        assertTrue(branch2.containsKey(GraphSONTokens.KEY));
        assertTrue(branch2.containsKey(GraphSONTokens.VALUE));

        final Map.Entry entry = branch2.get(GraphSONTokens.VALUE).entrySet().iterator().next();
        final HashMap<String, HashMap<String, Object>> branch2Prop = (HashMap<String, HashMap<String, Object>>) entry.getValue();
        assertTrue(branch2Prop.get(GraphSONTokens.KEY).containsKey(GraphSONTokens.ID));
        assertTrue(branch2Prop.get(GraphSONTokens.KEY).containsKey(GraphSONTokens.VALUE));
        assertTrue(branch2Prop.get(GraphSONTokens.KEY).containsKey(GraphSONTokens.LABEL));
        assertEquals("name", branch2Prop.get(GraphSONTokens.KEY).get(GraphSONTokens.LABEL));
        assertEquals("vadas", branch2Prop.get(GraphSONTokens.KEY).get(GraphSONTokens.VALUE));
        assertEquals(entry.getKey().toString(), branch2Prop.get(GraphSONTokens.KEY).get(GraphSONTokens.ID).toString());
    }
}
