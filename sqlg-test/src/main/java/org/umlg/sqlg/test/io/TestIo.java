package org.umlg.sqlg.test.io;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.WriteTest;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgIoRegistryV3;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/06/13
 */
public class TestIo extends BaseTest {

    private String ioType;

    private boolean assertViaDirectEquality;

    private boolean assertEdgesAtSameTimeAsVertex;

    private Function<Graph, GraphReader> readerMaker;

    private Function<Graph, GraphWriter> writerMaker;

    //Test are copied from ProcessStandardSuite. They fail there as the registery is not registered.
    @Test
    public void g_io_write_withXwrite_gryoX() throws IOException {
        loadModern();
        final String fileToWrite = TestHelper.generateTempFile(WriteTest.class, "tinkerpop-modern-v3d0", ".kryo").getAbsolutePath().replace('\\', '/');
        final File f = new File(fileToWrite);
        assertThat(f.length() == 0, is(true));
        final Traversal<Object, Object> traversal = this.sqlgGraph.traversal().io(fileToWrite)
                .with(IO.writer, IO.gryo)
                .with(IO.registry, SqlgIoRegistryV3.instance())
                .write();
        printTraversalForm(traversal);
        traversal.iterate();
        assertThat(f.length() > 0, is(true));
    }

    @Test
    public void g_io_write_withXwriter_graphsonX() throws IOException {
        loadModern();
        final String fileToWrite = TestHelper.generateTempFile(WriteTest.class, "tinkerpop-modern-v3d0", ".json").getAbsolutePath().replace('\\', '/');

        final File f = new File(fileToWrite);
        assertThat(f.length() == 0, is(true));

        final Traversal<Object, Object> traversal = this.sqlgGraph.traversal().io(fileToWrite)
                .with(IO.writer, IO.graphson)
                .with(IO.registry, SqlgIoRegistryV3.instance())
                .write();
        printTraversalForm(traversal);
        traversal.iterate();

        assertThat(f.length() > 0, is(true));
    }

    @Test
    public void g_io_writeXjsonX() throws IOException {
        loadModern();
        final String fileToWrite = TestHelper.generateTempFile(WriteTest.class,"tinkerpop-modern-v3d0", ".json").getAbsolutePath().replace('\\', '/');

        final File f = new File(fileToWrite);
        assertThat(f.length() == 0, is(true));

        final Traversal<Object,Object> traversal =  this.sqlgGraph.traversal()
                .io(fileToWrite)
                .with(IO.writer, IO.graphson)
                .with(IO.registry, SqlgIoRegistryV3.instance())
                .write();
        printTraversalForm(traversal);
        traversal.iterate();

        assertThat(f.length() > 0, is(true));
    }

    @Test
    public void g_io_writeXkryoX() throws IOException {
        loadModern();
        final String fileToWrite = TestHelper.generateTempFile(WriteTest.class, "tinkerpop-modern-v3d0", ".kryo").getAbsolutePath().replace('\\', '/');

        final File f = new File(fileToWrite);
        assertThat(f.length() == 0, is(true));

        final Traversal<Object,Object> traversal = this.sqlgGraph.traversal()
                .io(fileToWrite)
                .with(IO.writer, IO.gryo)
                .with(IO.registry, SqlgIoRegistryV3.instance())
                .write();
        printTraversalForm(traversal);
        traversal.iterate();

        assertThat(f.length() > 0, is(true));
    }

    @Test
    public void shouldReadWriteVertexWithBOTHEdges() throws Exception {
        this.ioType = "graphson-v1-embedded";
        this.assertViaDirectEquality = true;
        this.assertEdgesAtSameTimeAsVertex = false;
        this.readerMaker = g -> g.io(IoCore.graphson()).reader().mapper(g.io(IoCore.graphson()).mapper().create()).create();
        this.writerMaker = g -> g.io(IoCore.graphson()).writer().mapper(g.io(IoCore.graphson()).mapper().create()).create();

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
                        Assert.assertEquals(v1.id(), graph.vertices(detachedVertex.id().toString()).next().id());
                        Assert.assertEquals(v1.label(), detachedVertex.label());
                        Assert.assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                        Assert.assertEquals("marko", detachedVertex.value("name"));
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
                            Assert.assertEquals(e1.id(), graph.edges(detachedEdge.id().toString()).next().id());
                            Assert.assertEquals(v1.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                            Assert.assertEquals(v2.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                            Assert.assertEquals(v2.label(), detachedEdge.inVertex().label());
                            Assert.assertEquals(e1.label(), detachedEdge.label());
                            Assert.assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                            Assert.assertEquals(0.5d, detachedEdge.value("weight"), 0.000001d);
                        }
                        calledEdge1.set(true);
                    } else if (matcher.test(e2)) {
                        if (assertViaDirectEquality) {
                            TestHelper.validateEdgeEquality(e2, detachedEdge);
                        } else {
                            Assert.assertEquals(e2.id(), graph.edges(detachedEdge.id().toString()).next().id());
                            Assert.assertEquals(v2.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                            Assert.assertEquals(v1.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                            Assert.assertEquals(v1.label(), detachedEdge.outVertex().label());
                            Assert.assertEquals(e2.label(), detachedEdge.label());
                            Assert.assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                            Assert.assertEquals(1.0d, detachedEdge.value("weight"), 0.000001d);
                        }
                        calledEdge2.set(true);
                    } else {
                        Assert.fail("An edge id generated that does not exist");
                    }

                    return null;
                }, Direction.BOTH);
            }

            Assert.assertTrue(calledVertex.get());
            Assert.assertTrue(calledEdge1.get());
            Assert.assertTrue(calledEdge2.get());
        }
    }

    @Test
    public void shouldReadWriteVertexWithBOTHEdgesUserSuppliedPK() throws Exception {

        this.ioType = "graphson-v1-embedded";
        this.assertViaDirectEquality = true;
        this.assertEdgesAtSameTimeAsVertex = false;
        this.readerMaker = g -> g.io(IoCore.graphson()).reader().mapper(g.io(IoCore.graphson()).mapper().create()).create();
        this.writerMaker = g -> g.io(IoCore.graphson()).writer().mapper(g.io(IoCore.graphson()).mapper().create()).create();

        Graph graph = this.sqlgGraph;
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "person",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                            put("uid2", PropertyType.varChar(100));
                            put("name", PropertyType.STRING);
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        personVertexLabel.ensureEdgeLabelExist(
                "friends",
                personVertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("weight", PropertyType.DOUBLE);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();
        final Vertex v1 = graph.addVertex("name", "marko", T.label, "person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());

        final Vertex v2 = graph.addVertex(T.label, "person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5d, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0d, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());

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
                        Assert.assertEquals(v1.id(), graph.vertices(detachedVertex.id().toString()).next().id());
                        Assert.assertEquals(v1.label(), detachedVertex.label());
                        Assert.assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                        Assert.assertEquals("marko", detachedVertex.value("name"));
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
                            Assert.assertEquals(e1.id(), graph.edges(detachedEdge.id().toString()).next().id());
                            Assert.assertEquals(v1.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                            Assert.assertEquals(v2.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                            Assert.assertEquals(v2.label(), detachedEdge.inVertex().label());
                            Assert.assertEquals(e1.label(), detachedEdge.label());
                            Assert.assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                            Assert.assertEquals(0.5d, detachedEdge.value("weight"), 0.000001d);
                        }
                        calledEdge1.set(true);
                    } else if (matcher.test(e2)) {
                        if (assertViaDirectEquality) {
                            TestHelper.validateEdgeEquality(e2, detachedEdge);
                        } else {
                            Assert.assertEquals(e2.id(), graph.edges(detachedEdge.id().toString()).next().id());
                            Assert.assertEquals(v2.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                            Assert.assertEquals(v1.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                            Assert.assertEquals(v1.label(), detachedEdge.outVertex().label());
                            Assert.assertEquals(e2.label(), detachedEdge.label());
                            Assert.assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                            Assert.assertEquals(1.0d, detachedEdge.value("weight"), 0.000001d);
                        }
                        calledEdge2.set(true);
                    } else {
                        Assert.fail("An edge id generated that does not exist");
                    }

                    return null;
                }, Direction.BOTH);
            }

            Assert.assertTrue(calledVertex.get());
            Assert.assertTrue(calledEdge1.get());
            Assert.assertTrue(calledEdge2.get());
        }
    }
}
