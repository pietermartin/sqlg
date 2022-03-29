package org.umlg.sqlg.test.io;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/09/03
 */
@RunWith(Parameterized.class)
public class TestIoEdge extends BaseTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"graphson-v1", false, false,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V1_0)).reader().create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V1_0)).writer().create()},
                {"graphson-v1-embedded", true, true,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V1_0)).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V1_0)).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create()).create()},
                {"graphson-v2", false, false,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V2_0)).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.NO_TYPES).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V2_0)).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.NO_TYPES).create()).create()},
                {"graphson-v2-embedded", true, true,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V2_0)).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V2_0)).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create()).create()},
                {"graphson-v3", true, true,
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
    public boolean assertIdDirectly;

    @Parameterized.Parameter(value = 2)
    public boolean assertDouble;

    @Parameterized.Parameter(value = 3)
    public Function<Graph, GraphReader> readerMaker;

    @Parameterized.Parameter(value = 4)
    public Function<Graph, GraphWriter> writerMaker;

    @Test
    public void shouldReadWriteEdge() throws Exception {
        final Vertex v1 = this.sqlgGraph.addVertex(T.label, "person");
        final Vertex v2 = this.sqlgGraph.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friend", v2, "weight", 0.5d, "acl", "rw");

        assertEdge(v1, v2, e, true);
    }

    @Test
    public void shouldReadWriteEdgeUserSuppliedPK() throws Exception {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().
                ensureVertexLabelExist(
                        "person",
                        new HashMap<>() {{
                            put("uid1", PropertyDefinition.of(PropertyType.varChar(100)));
                            put("uid2", PropertyDefinition.of(PropertyType.varChar(100)));
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        personVertexLabel.ensureEdgeLabelExist(
                "friend",
                personVertexLabel,
                new HashMap<>() {{
                    put("uid1", PropertyDefinition.of(PropertyType.varChar(100)));
                    put("uid2", PropertyDefinition.of(PropertyType.varChar(100)));
                    put("weight", PropertyDefinition.of(PropertyType.DOUBLE));
                    put("acl", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        final Vertex v1 = this.sqlgGraph.addVertex(T.label, "person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        final Vertex v2 = this.sqlgGraph.addVertex(T.label, "person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        final Edge e = v1.addEdge("friend", v2, "weight", 0.5d, "acl", "rw", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());

        assertEdge(v1, v2, e, true);
    }

    @Test
    public void shouldReadWriteDetachedEdge() throws Exception {
        final Vertex v1 = this.sqlgGraph.addVertex(T.label, "person");
        final Vertex v2 = this.sqlgGraph.addVertex(T.label, "person");
        final Edge e = DetachedFactory.detach(v1.addEdge("friend", v2, "weight", 0.5d, "acl", "rw"), true);

        assertEdge(v1, v2, e, true);
    }

    @Test
    public void shouldReadWriteDetachedEdgeAsReference() throws Exception {
        final Vertex v1 = this.sqlgGraph.addVertex(T.label, "person");
        final Vertex v2 = this.sqlgGraph.addVertex(T.label, "person");
        final Edge e = DetachedFactory.detach(v1.addEdge("friend", v2, "weight", 0.5d, "acl", "rw"), false);

        assertEdge(v1, v2, e, false);
    }

    private void assertEdge(final Vertex v1, final Vertex v2, final Edge e, final boolean assertProperties) throws IOException {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphWriter writer = writerMaker.apply(this.sqlgGraph);
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphReader reader = readerMaker.apply(this.sqlgGraph);
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, edge -> {
                    final Edge detachedEdge = (Edge) edge;
                    Assert.assertEquals(e.id(), assertIdDirectly ? detachedEdge.id() : this.sqlgGraph.edges(detachedEdge.id().toString()).next().id());
                    Assert.assertEquals(v1.id(), assertIdDirectly ? detachedEdge.outVertex().id() : this.sqlgGraph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                    Assert.assertEquals(v2.id(), assertIdDirectly ? detachedEdge.inVertex().id() : this.sqlgGraph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                    Assert.assertEquals(v1.label(), detachedEdge.outVertex().label());
                    Assert.assertEquals(v2.label(), detachedEdge.inVertex().label());
                    Assert.assertEquals(e.label(), detachedEdge.label());

                    if (assertProperties) {
                        Assert.assertEquals(assertDouble ? 0.5d : 0.5f, e.properties("weight").next().value());
                        Assert.assertEquals("rw", e.properties("acl").next().value());
                    } else {
                        Assert.assertEquals(e.keys().size(), IteratorUtils.count(detachedEdge.properties()));
                    }

                    called.set(true);
                    return null;
                });
            }

            Assert.assertTrue(called.get());
        }
    }
}
