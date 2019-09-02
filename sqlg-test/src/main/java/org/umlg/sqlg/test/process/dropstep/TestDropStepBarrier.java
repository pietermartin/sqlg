package org.umlg.sqlg.test.process.dropstep;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.MutationListener;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.*;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/11/21
 */
@RunWith(Parameterized.class)
public class TestDropStepBarrier extends BaseTest {

    @Parameterized.Parameter
    public Boolean fkOn;
    @Parameterized.Parameter(1)
    public Boolean mutatingCallback;
    private final List<Vertex> removedVertices = new ArrayList<>();
    private final List<Edge> removedEdges = new ArrayList<>();
    private final List<VertexProperty> removedVertexProperties = new ArrayList<>();
    private final List<Property> removedEdgeProperties = new ArrayList<>();
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

                @Override
                public void edgePropertyRemoved(final Edge element, final Property property) {
                    removedEdgeProperties.add(property);

                }

                @Override
                public void vertexPropertyRemoved(final VertexProperty property) {
                    removedVertexProperties.add(property);
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
    public void testDropBarrier() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        c1.addEdge("ca", a1);
        this.sqlgGraph.tx().commit();

        this.dropTraversal.V().local(__.hasLabel("A")).drop().iterate();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.traversal().V().hasLabel("A").hasNext());
        Assert.assertFalse(this.sqlgGraph.traversal().E().hasLabel("ab", "ca").hasNext());
        Assert.assertTrue(this.sqlgGraph.traversal().V().hasLabel("B").hasNext());
        Assert.assertTrue(this.sqlgGraph.traversal().V().hasLabel("C").hasNext());
        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertEquals(2, this.removedEdges.size());
        }
    }

    @Test
    public void testDropBarrierUserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        cVertexLabel.ensureEdgeLabelExist(
                "ca",
                aVertexLabel,
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        c1.addEdge("ca", a1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        this.dropTraversal.V().local(__.hasLabel("A")).drop().iterate();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.traversal().V().hasLabel("A").hasNext());
        Assert.assertFalse(this.sqlgGraph.traversal().E().hasLabel("ab", "ca").hasNext());
        Assert.assertTrue(this.sqlgGraph.traversal().V().hasLabel("B").hasNext());
        Assert.assertTrue(this.sqlgGraph.traversal().V().hasLabel("C").hasNext());
        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertEquals(2, this.removedEdges.size());
        }
    }

    @Test
    public void unontimizedOptional() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        b1.addEdge("bc", c1);
        b2.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").as("a")
                .optional(
                        __.select("a").out()
                ).out()
                .toList();

        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(c1, c3)));

        this.dropTraversal.V().hasLabel("A").as("a")
                .optional(
                        __.select("a").out()
                )
                .drop()
                .hasNext();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("B").count().next(), 0);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("B").count().next(), 0);
        if (this.mutatingCallback) {
            Assert.assertEquals(4, this.removedVertices.size());
            Assert.assertEquals(5, this.removedEdges.size());
        }
    }

    @Test
    public void unontimizedOptionaluserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        bVertexLabel.ensureEdgeLabelExist(
                "bc",
                cVertexLabel,
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        a1.addEdge("ab", b2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        a1.addEdge("ab", b3, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b1.addEdge("bc", c1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b2.addEdge("bc", c3, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").as("a")
                .optional(
                        __.select("a").out()
                ).out()
                .toList();

        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(c1, c3)));

        this.dropTraversal.V().hasLabel("A").as("a")
                .optional(
                        __.select("a").out()
                )
                .drop()
                .hasNext();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("B").count().next(), 0);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("B").count().next(), 0);
        if (this.mutatingCallback) {
            Assert.assertEquals(4, this.removedVertices.size());
            Assert.assertEquals(5, this.removedEdges.size());
        }
    }

    @Test
    public void testOptionalDrop() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .optional(
                        __.out()
                )
                .toList();
        Assert.assertEquals(4, vertices.size());

       this.dropTraversal
                .V().hasLabel("A")
                .optional(
                        __.out()
                )
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().count().next(), 0);
        if (this.mutatingCallback) {
            Assert.assertEquals(4, this.removedVertices.size());
            Assert.assertEquals(3, this.removedEdges.size());
        }
    }

    @Test
    public void testOptionalDropuserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        a1.addEdge("ab", b2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        a1.addEdge("ab", b3, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .optional(
                        __.out()
                )
                .toList();
        Assert.assertEquals(4, vertices.size());

        this.dropTraversal
                .V().hasLabel("A")
                .optional(
                        __.out()
                )
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().count().next(), 0);
        if (this.mutatingCallback) {
            Assert.assertEquals(4, this.removedVertices.size());
            Assert.assertEquals(3, this.removedEdges.size());
        }
    }

    @Test
    public void testEdgePropertyDropuserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("uid3", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2", "uid3"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("uid3", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2", "uid3"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("uid3", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2", "uid3"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "name", "e1", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "name", "e2", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        List<Edge> edges = this.sqlgGraph.traversal().E().hasLabel("ab").toList();
        Assert.assertEquals(2, edges.size());

        this.dropTraversal.E().hasLabel("ab").has("name", "e1").properties("name").drop().iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraph.traversal().E().count().next(), 0);
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().has("name", "e1").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().has("name", "e2").count().next(), 0);

        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedEdgeProperties.size());
            Assert.assertEquals(0, this.removedVertexProperties.size());
        }
    }

    @Test
    public void testEdgePropertyDrop() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("uid3", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2", "uid3"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("uid3", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2", "uid3"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "name", "e1", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "name", "e2", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        List<? extends Object> names = this.sqlgGraph.traversal().E().hasLabel("ab").has("name", "e1").values("name").toList();
        names = this.sqlgGraph.traversal().E().hasLabel("ab").has("name", "e1").properties("name").toList();
        this.dropTraversal.E().hasLabel("ab").has("name", "e1").properties("name").drop().iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraph.traversal().E().count().next(), 0);
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().has("name", "e1").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().has("name", "e2").count().next(), 0);

        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedEdgeProperties.size());
            Assert.assertEquals(0, this.removedVertexProperties.size());
        }

    }

    @Test
    public void playlistPaths() {
        Assume.assumeTrue(!isMsSqlServer());
        loadGratefulDead();
        final GraphTraversal<Vertex, Vertex> traversal = getPlaylistPaths(this.sqlgGraph.traversal());
        printTraversalForm(traversal);
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(100, vertices.size());
        getPlaylistPaths(this.dropTraversal).barrier().drop().iterate();
        this.sqlgGraph.tx().commit();
        Long count = this.sqlgGraph.traversal().V().count().next();
        //Sometimes its 804 and sometimes 803.
        //Probably something to do with the limit
        Assert.assertTrue(count == 804 || count == 803);
    }

    private GraphTraversal<Vertex, Vertex> getPlaylistPaths(GraphTraversalSource graphTraversal) {
        return graphTraversal.V().has("name", "Bob_Dylan").in("sungBy").as("a").
                repeat(__.out().order().by(Order.shuffle).simplePath().from("a")).
                until(__.out("writtenBy").has("name", "Johnny_Cash")).limit(1).as("b").
                repeat(__.out().order().by(Order.shuffle).as("c").simplePath().from("b").to("c")).
                until(__.out("sungBy").has("name", "Grateful_Dead")).limit(100);
    }

    @Test
    public void dropProperty() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexPropertyRemoved(final VertexProperty element) {
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);
        final EventStrategy eventStrategy = builder.create();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.traversal().withStrategies(eventStrategy).V().properties().drop().iterate();
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(triggered.get());
        Assert.assertFalse(this.sqlgGraph.traversal().V().hasLabel("A").has("name").hasNext());
    }

    @Test
    public void dropPropertyuserSuppliedIds() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexPropertyRemoved(final VertexProperty element) {
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);
        final EventStrategy eventStrategy = builder.create();
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("uid3", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("uid3", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.traversal().withStrategies(eventStrategy).V().properties("name").drop().iterate();
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(triggered.get());
        Assert.assertFalse(this.sqlgGraph.traversal().V().hasLabel("A").has("name").hasNext());
    }

    @Test
    public void multiplePathQueriesDrop() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4");
        Vertex a5 = this.sqlgGraph.addVertex(T.label, "A", "name", "a5");
        Vertex a6 = this.sqlgGraph.addVertex(T.label, "A", "name", "a6");
        a1.addEdge("aa", a2);
        a2.addEdge("aa", a3);
        a3.addEdge("aa", a4);
        a4.addEdge("aa", a5);
        a5.addEdge("aa", a6);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").out().out().toList();
        Assert.assertEquals(4, vertices.size());
        this.dropTraversal.V().hasLabel("A").out().out().drop().iterate();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0L);

        if (this.mutatingCallback) {
            Assert.assertEquals(4, this.removedVertices.size());
            Assert.assertEquals(4, this.removedEdges.size());
        }
    }

    @Test
    public void multiplePathQueriesDropuserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.varChar(100));
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("uid3", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "aa",
                aVertexLabel,
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("uid3", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        Vertex a5 = this.sqlgGraph.addVertex(T.label, "A", "name", "a5", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        Vertex a6 = this.sqlgGraph.addVertex(T.label, "A", "name", "a6", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        a1.addEdge("aa", a2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        a2.addEdge("aa", a3, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        a3.addEdge("aa", a4, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        a4.addEdge("aa", a5, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        a5.addEdge("aa", a6, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "uid3", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").out().out().toList();
        Assert.assertEquals(4, vertices.size());
        this.dropTraversal.V().hasLabel("A").out().out().drop().iterate();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0L);

        if (this.mutatingCallback) {
            Assert.assertEquals(4, this.removedVertices.size());
            Assert.assertEquals(4, this.removedEdges.size());
        }
    }

    @Test
    public void testDropEdges() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        this.dropTraversal.V().hasLabel("A").drop().iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().count().next(), 0);
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().count().next(), 0);

        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertEquals(1, this.removedEdges.size());
        }
    }

    @Test
    public void testDropEdgesUserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.varChar(100));
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.varChar(100));
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.varChar(100));
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        this.dropTraversal.V().hasLabel("A").drop().iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().count().next(), 0);
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().count().next(), 0);

        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertEquals(1, this.removedEdges.size());
        }
    }

    @Test
    public void testDropWithSelect() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1", "status", 1);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2", "status", 0);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3", "status", 1);
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        this.sqlgGraph.tx().commit();

        List<Edge> ees = this.sqlgGraph.traversal().V().hasLabel("A").outE("ab").as("e").otherV().has("status", 1).<Edge>select("e").toList();
        Assert.assertEquals(2, ees.size());
        this.sqlgGraph.traversal().V().hasLabel("A").outE("ab").as("e").otherV().has("status", 1).select("e").drop().iterate();
        this.sqlgGraph.tx().commit();
        ees = this.sqlgGraph.traversal().V().hasLabel("A").outE("ab").as("e").otherV().has("status", 1).<Edge>select("e").toList();
        Assert.assertEquals(0, ees.size());
    }

    @Test
    public void testDropWithSelectAndUserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.varChar(100));
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.varChar(100));
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.varChar(100));
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "status", 1);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "status", 0);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "status", 1);
        a1.addEdge("ab", b1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        a1.addEdge("ab", b2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        a1.addEdge("ab", b3, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        List<Edge> ees = this.sqlgGraph.traversal().V().hasLabel("A").outE("ab").as("e").otherV().has("status", 1).<Edge>select("e").toList();
        Assert.assertEquals(2, ees.size());
        this.sqlgGraph.traversal().V().hasLabel("A").outE("ab").as("e").otherV().has("status", 1).select("e").drop().iterate();
        this.sqlgGraph.tx().commit();
        ees = this.sqlgGraph.traversal().V().hasLabel("A").outE("ab").as("e").otherV().has("status", 1).<Edge>select("e").toList();
        Assert.assertEquals(0, ees.size());

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
