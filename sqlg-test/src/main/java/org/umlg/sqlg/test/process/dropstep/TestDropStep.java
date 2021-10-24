package org.umlg.sqlg.test.process.dropstep;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.MutationListener;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/11/11
 */
@SuppressWarnings({"DuplicatedCode", "unchecked", "unused", "rawtypes"})
@RunWith(Parameterized.class)
public class TestDropStep extends BaseTest {

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
    public void testUserSuppliedIdsWithinID() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "PolicyDiscrepancy",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "PolicyDiscrepancy",
                    "uid1", "uid1" + i,
                    "uid2", "uid2" + i,
                    "name", "name" + i
            );
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("PolicyDiscrepancy").toList().size(), 0);

        sqlgGraph.traversal().V()
                .hasLabel("PolicyDiscrepancy")
                .has("uid1", "uid10")
                .has("uid2", "uid20")
                .drop().iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(9, this.sqlgGraph.traversal().V().hasLabel("PolicyDiscrepancy").toList().size(), 0);

        List<String> uids = Arrays.asList("uid11", "uid12", "uid13");
        sqlgGraph.traversal().V()
                .hasLabel("PolicyDiscrepancy")
                .has("uid1", P.within(uids))
                .drop().iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(6, this.sqlgGraph.traversal().V().hasLabel("PolicyDiscrepancy").toList().size(), 0);
    }

    @Test
    public void testNormalAndUserSuppliedIdsIn() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.STRING);
                }}
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid", "uid1", "name", "name1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "name1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "name2");
        a1.addEdge("ab", b1, "uid", "uid1");
        a1.addEdge("ab", b2, "uid", "uid2");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("A").out().toList().size(), 0);
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("B").toList().size(), 0);


        this.sqlgGraph.traversal().V()
                .hasLabel("B")
                .has("name", P.within("name1"))
                .drop().iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("B").toList().size(), 0);
    }

    @Test
    public void testNormalAndUserSuppliedIdsOut() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.STRING);
                }}
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "name1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B","uid", "uid1",  "name", "name1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B","uid", "uid2",  "name", "name2");
        a1.addEdge("ab", b1, "uid", "uid1");
        a1.addEdge("ab", b2, "uid", "uid2");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("A").out().toList().size(), 0);
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("B").toList().size(), 0);


        this.sqlgGraph.traversal().V()
                .hasLabel("A")
                .has("name", P.within("name1"))
                .drop().iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("A").toList().size(), 0);
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("B").toList().size(), 0);
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().hasLabel("ab").toList().size(), 0);
    }

    @Test
    public void testUserSuppliedDrop() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.STRING);
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();

        String uid1 = UUID.randomUUID().toString();
        String uid2 = UUID.randomUUID().toString();
        this.sqlgGraph.addVertex(T.label, "A", "uid1", uid1, "uid2", uid2, "name", "a1");
        this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a2");
        this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a3");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(3, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);

        SchemaTable schemaTable = SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "A");
        this.sqlgGraph.traversal().V().hasLabel("A")
                .hasId(RecordId.from(sqlgGraph, schemaTable.getSchema() + "." + schemaTable.getTable() + RecordId.RECORD_ID_DELIMITER + "[" + uid1 + "," + uid2 + "]"))
                .drop().iterate();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);

        this.sqlgGraph.addVertex(T.label, "A", "uid1", uid1, "uid2", uid2, "name", "a1");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(3, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);

        this.sqlgGraph.traversal().V().hasLabel("A")
                .has("name", "a1")
                .drop().iterate();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
    }

    @Test
    public void g_V_properties_drop() {
        loadModern();
        final Traversal<Vertex, VertexProperty<?>> traversal = (Traversal) this.sqlgGraph.traversal().V().properties().drop();
        printTraversalForm(traversal);
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(6, IteratorUtils.count(this.sqlgGraph.traversal().V()));
        Assert.assertEquals(6, IteratorUtils.count(this.sqlgGraph.traversal().E()));
        this.sqlgGraph.traversal().V().forEachRemaining(vertex -> Assert.assertEquals(0, IteratorUtils.count(vertex.properties())));
    }

    @Test
    public void shouldReferenceVertexWhenRemoved() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = this.sqlgGraph.addVertex();
        final String label = v.label();
        final Object id = v.id();

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexRemoved(final Vertex element) {
                MatcherAssert.assertThat(element, IsInstanceOf.instanceOf(ReferenceVertex.class));
                Assert.assertEquals(id, element.id());
                Assert.assertEquals(label, element.label());
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener)
            .detach(EventStrategy.Detachment.REFERENCE);

        if (this.sqlgGraph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(this.sqlgGraph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = this.sqlgGraph.traversal().withStrategies(eventStrategy);

        gts.V(v).drop().iterate();
        this.sqlgGraph.tx().commit();

        AbstractGremlinTest.assertVertexEdgeCounts(this.sqlgGraph, 0, 0);
        MatcherAssert.assertThat(triggered.get(), CoreMatchers.is(true));
    }

    @Test
    public void shouldReferenceVertexWhenRemovedUserSuppliedIds() {

        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2")));
        this.sqlgGraph.tx().commit();

        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = this.sqlgGraph.addVertex(T.label, "A", "uid1", "1", "uid2", "2");
        final String label = v.label();
        final Object id = v.id();

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexRemoved(final Vertex element) {
                MatcherAssert.assertThat(element, IsInstanceOf.instanceOf(ReferenceVertex.class));
                Assert.assertEquals(id, element.id());
                Assert.assertEquals(label, element.label());
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener)
            .detach(EventStrategy.Detachment.REFERENCE);

        if (this.sqlgGraph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(this.sqlgGraph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = this.sqlgGraph.traversal().withStrategies(eventStrategy);

        gts.V(v).drop().iterate();
        this.sqlgGraph.tx().commit();

        AbstractGremlinTest.assertVertexEdgeCounts(this.sqlgGraph, 0, 0);
        MatcherAssert.assertThat(triggered.get(), CoreMatchers.is(true));
    }

    @Test
    public void testDropStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        this.sqlgGraph.tx().commit();

        this.dropTraversal.V().hasLabel("A").has("name", "a1").drop().iterate();
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(a2, a3)));
        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertTrue(this.removedVertices.contains(a1));
        }
    }

    @Test
    public void testDropStepUserSuppliedIds() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.STRING);
                    put("uid", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a3");
        this.sqlgGraph.tx().commit();

        this.dropTraversal.V().hasLabel("A").has("name", "a1").drop().iterate();
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(a2, a3)));
        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertTrue(this.removedVertices.contains(a1));
        }
    }

    @Test
    public void testDropStepRepeat1() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        c1.addEdge("cd", d1);
        this.sqlgGraph.tx().commit();

        this.dropTraversal.V().hasLabel("A")
                .repeat(__.out())
                .times(3)
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertFalse(this.sqlgGraph.traversal().V().hasLabel("D").hasNext());
        Assert.assertFalse(this.sqlgGraph.traversal().E().hasLabel("cd").hasNext());
        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertEquals(1, this.removedEdges.size());
        }
    }

    @Test
    public void testDropStepRepeat1UserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel dVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "D",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        bVertexLabel.ensureEdgeLabelExist(
                "bc",
                cVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        cVertexLabel.ensureEdgeLabelExist(
                "cd",
                dVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b1.addEdge("bc", c1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        c1.addEdge("cd", d1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        this.dropTraversal.V().hasLabel("A")
                .repeat(__.out())
                .times(3)
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertFalse(this.sqlgGraph.traversal().V().hasLabel("D").hasNext());
        Assert.assertFalse(this.sqlgGraph.traversal().E().hasLabel("cd").hasNext());
        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertEquals(1, this.removedEdges.size());
        }
    }


    @Test
    public void testDuplicatePath() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        a1.addEdge("aaa", a2);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").out().toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testAsWithDuplicatePaths() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a2");
        Edge e1 = a1.addEdge("friend", a2, "weight", 5);
        this.sqlgGraph.tx().commit();

        testAsWithDuplicatePaths_assert(this.sqlgGraph, a1, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testAsWithDuplicatePaths_assert(this.sqlgGraph1, a1, e1);
        }
    }

    private void testAsWithDuplicatePaths_assert(SqlgGraph sqlgGraph, Vertex a1, Edge e1) {
        DefaultGraphTraversal<Vertex, Map<String, Element>> gt = (DefaultGraphTraversal<Vertex, Map<String, Element>>) sqlgGraph.traversal()
                .V(a1)
                .outE().as("e")
                .inV()
                .in().as("v")
                .<Element>select("e", "v");
        Assert.assertEquals(5, gt.getSteps().size());

        List<Map<String, Element>> result = gt.toList();
        Assert.assertEquals(2, gt.getSteps().size());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(e1, result.get(0).get("e"));
        Assert.assertEquals(a1, result.get(0).get("v"));
    }


    @Test
    public void testDropStepRepeat2() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        c1.addEdge("cd", d1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D");
        b2.addEdge("ab", a2);
        c2.addEdge("bc", b2);
        d2.addEdge("cd", c2);
        this.sqlgGraph.tx().commit();

        this.dropTraversal.V().hasLabel("A")
                .repeat(__.out()).times(3)
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("D").count().next(), 0);

        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertEquals(1, this.removedEdges.size());
        }

        this.dropTraversal.V().hasLabel("A")
                .repeat(__.in()).times(3)
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("D").count().next(), 0);
        if (this.mutatingCallback) {
            Assert.assertEquals(2, this.removedVertices.size());
            Assert.assertEquals(2, this.removedEdges.size());
        }
    }

    @Test
    public void testDropStepRepeat2UserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel dVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "D",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        bVertexLabel.ensureEdgeLabelExist(
                "bc",
                cVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        cVertexLabel.ensureEdgeLabelExist(
                "cd",
                dVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );

        bVertexLabel.ensureEdgeLabelExist(
                "ab",
                aVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        cVertexLabel.ensureEdgeLabelExist(
                "bc",
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        dVertexLabel.ensureEdgeLabelExist(
                "cd",
                cVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        a1.addEdge("ab", b1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b1.addEdge("bc", c1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        c1.addEdge("cd", d1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b2.addEdge("ab", a2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        c2.addEdge("bc", b2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        d2.addEdge("cd", c2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        this.dropTraversal.V().hasLabel("A")
                .repeat(__.out()).times(3)
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("D").count().next(), 0);

        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertEquals(1, this.removedEdges.size());
        }

        this.dropTraversal.V().hasLabel("A")
                .repeat(__.in()).times(3)
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("D").count().next(), 0);
        if (this.mutatingCallback) {
            Assert.assertEquals(2, this.removedVertices.size());
            Assert.assertEquals(2, this.removedEdges.size());
        }
    }

    @Test
    public void testDropStepWithJoin() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Edge e1 = a1.addEdge("ab", b1);
        Edge e2 = a1.addEdge("ab", b2);
        Edge e3 = a1.addEdge("ab", b3);
        Edge e4 = b1.addEdge("bc", c1);
        Edge e5 = b2.addEdge("bc", c1);
        Edge e6 = b3.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();

        this.dropTraversal
                .V().hasLabel("A").as("a")
                .out("ab").has("name", "b2")
                .drop().iterate();
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").out("ab").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(b1, b3)));
        vertices = this.sqlgGraph.traversal().V().hasLabel("B").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(b1, b3)));
        List<Edge> edges = this.sqlgGraph.traversal().E().hasLabel("ab").toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertTrue(edges.containsAll(Arrays.asList(e1, e3)));

        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertTrue(this.removedVertices.contains(b2));
            Assert.assertTrue(this.removedEdges.contains(e2));
        }
    }

    @Test
    public void testDropStepWithJoinWithuserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        bVertexLabel.ensureEdgeLabelExist(
                "bc",
                cVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "c1");
        Edge e1 = a1.addEdge("ab", b1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Edge e2 = a1.addEdge("ab", b2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Edge e3 = a1.addEdge("ab", b3, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Edge e4 = b1.addEdge("bc", c1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Edge e5 = b2.addEdge("bc", c1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Edge e6 = b3.addEdge("bc", c1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        this.dropTraversal
                .V().hasLabel("A").as("a")
                .out("ab").has("name", "b2")
                .drop().iterate();
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").out("ab").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(b1, b3)));
        vertices = this.sqlgGraph.traversal().V().hasLabel("B").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(b1, b3)));
        List<Edge> edges = this.sqlgGraph.traversal().E().hasLabel("ab").toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertTrue(edges.containsAll(Arrays.asList(e1, e3)));

        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertTrue(this.removedVertices.contains(b2));
            Assert.assertTrue(this.removedEdges.contains(e2));
        }
    }

    @Test
    public void testDropEdges() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Edge e1 = a1.addEdge("ab", b1);
        Edge e2 = a1.addEdge("ab", b2);
        Edge e3 = a1.addEdge("ab", b3);
        Edge e4 = b1.addEdge("bc", c1);
        Edge e5 = b2.addEdge("bc", c1);
        Edge e6 = b3.addEdge("bc", c1);

        this.sqlgGraph.tx().commit();

        this.dropTraversal
                .V().hasLabel("A").as("a")
                .outE("ab")
                .drop().iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertFalse(this.sqlgGraph.traversal().E().hasLabel("ab").hasNext());
        Assert.assertEquals(3L, this.sqlgGraph.traversal().E().hasLabel("bc").count().next(), 0L);
        if (this.mutatingCallback) {
            Assert.assertEquals(3, this.removedEdges.size());
            Assert.assertEquals(0, this.removedVertices.size());
            Assert.assertTrue(this.removedEdges.containsAll(Arrays.asList(e1, e2, e3)));
        }
    }

    @Test
    public void testDropEdgesUserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );
        bVertexLabel.ensureEdgeLabelExist(
                "bc",
                cVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "uid", UUID.randomUUID().toString(), "name", "c1");
        Edge e1 = a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString());
        Edge e2 = a1.addEdge("ab", b2, "uid", UUID.randomUUID().toString());
        Edge e3 = a1.addEdge("ab", b3, "uid", UUID.randomUUID().toString());
        Edge e4 = b1.addEdge("bc", c1, "uid", UUID.randomUUID().toString());
        Edge e5 = b2.addEdge("bc", c1, "uid", UUID.randomUUID().toString());
        Edge e6 = b3.addEdge("bc", c1, "uid", UUID.randomUUID().toString());

        this.sqlgGraph.tx().commit();

        this.dropTraversal
                .V().hasLabel("A").as("a")
                .outE("ab")
                .drop().iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertFalse(this.sqlgGraph.traversal().E().hasLabel("ab").hasNext());
        Assert.assertEquals(3L, this.sqlgGraph.traversal().E().hasLabel("bc").count().next(), 0L);
        if (this.mutatingCallback) {
            Assert.assertEquals(3, this.removedEdges.size());
            Assert.assertEquals(0, this.removedVertices.size());
            Assert.assertTrue(this.removedEdges.containsAll(Arrays.asList(e1, e2, e3)));
        }
    }

    @Test
    public void dropAll() {
        loadModern(this.sqlgGraph);
        this.dropTraversal.V().drop().iterate();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.dropTraversal.V().hasNext());
        Assert.assertEquals(0, IteratorUtils.count(this.sqlgGraph.traversal().V()));
        Assert.assertEquals(0, IteratorUtils.count(this.sqlgGraph.traversal().E()));
        if (this.mutatingCallback) {
            Assert.assertEquals(6, this.removedVertices.size());
            Assert.assertEquals(6, this.removedEdges.size());
        }
    }

    @Test
    public void dropAllEdges() {
        loadModern(this.sqlgGraph);
        this.dropTraversal.E().drop().iterate();
        Assert.assertFalse(this.dropTraversal.E().hasNext());
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(6, IteratorUtils.count(this.sqlgGraph.traversal().V()));
        Assert.assertEquals(0, IteratorUtils.count(this.sqlgGraph.traversal().E()));
        if (this.mutatingCallback) {
            Assert.assertEquals(0, this.removedVertices.size());
            Assert.assertEquals(6, this.removedEdges.size());
        }
    }


    @Test
    public void dropMultiplePathsToVertices() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
        Vertex c5 = this.sqlgGraph.addVertex(T.label, "C", "name", "c5");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        b2.addEdge("bc", c3);
        b2.addEdge("bc", c4);
        b2.addEdge("bc", c5);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A")
                .out()
                .out()
                .toList();
        Assert.assertEquals(6, vertices.size());
        Assert.assertTrue(vertices.removeAll(Arrays.asList(c1, c2, c3, c4, c5)));
        Assert.assertEquals(0, vertices.size());

        this.dropTraversal.V().hasLabel("A")
                .out()
                .out()
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(0L, this.sqlgGraph.traversal().V().hasLabel("C").count().next(), 0L);
        Assert.assertEquals(0L, this.sqlgGraph.traversal().E().hasLabel("bc").count().next(), 0L);

        if (this.mutatingCallback) {
            Assert.assertEquals(5, this.removedVertices.size());
            Assert.assertEquals(6, this.removedEdges.size());
        }

    }

    @Test
    public void dropMultiplePathsToVerticesUserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        bVertexLabel.ensureEdgeLabelExist(
                "bc",
                cVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "c3");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "c4");
        Vertex c5 = this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "c5");
        a1.addEdge("ab", b1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        a1.addEdge("ab", b2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b1.addEdge("bc", c1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b1.addEdge("bc", c2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b1.addEdge("bc", c3, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b2.addEdge("bc", c3, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b2.addEdge("bc", c4, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b2.addEdge("bc", c5, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A")
                .out()
                .out()
                .toList();
        Assert.assertEquals(6, vertices.size());
        Assert.assertTrue(vertices.removeAll(Arrays.asList(c1, c2, c3, c4, c5)));
        Assert.assertEquals(0, vertices.size());

        this.dropTraversal.V().hasLabel("A")
                .out()
                .out()
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(0L, this.sqlgGraph.traversal().V().hasLabel("C").count().next(), 0L);
        Assert.assertEquals(0L, this.sqlgGraph.traversal().E().hasLabel("bc").count().next(), 0L);

        if (this.mutatingCallback) {
            Assert.assertEquals(5, this.removedVertices.size());
            Assert.assertEquals(6, this.removedEdges.size());
        }

    }

    @Test
    public void dropMultiplePathsToVerticesWithHas() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
        Vertex c5 = this.sqlgGraph.addVertex(T.label, "C", "name", "c5");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        b2.addEdge("bc", c3);
        b2.addEdge("bc", c4);
        b2.addEdge("bc", c5);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A")
                .out()
                .out().has("name", P.within("c1", "c2", "c3", "c4"))
                .toList();
        Assert.assertEquals(5, vertices.size());
        Assert.assertTrue(vertices.removeAll(Arrays.asList(c1, c2, c3, c4)));
        Assert.assertEquals(0, vertices.size());

        this.dropTraversal.V().hasLabel("A")
                .out()
                .out().has("name", P.within("c1", "c2", "c3", "c4"))
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1L, this.sqlgGraph.traversal().V().hasLabel("C").count().next(), 0L);
        Assert.assertEquals(1L, this.sqlgGraph.traversal().E().hasLabel("bc").count().next(), 0L);

        if (this.mutatingCallback) {
            Assert.assertEquals(4, this.removedVertices.size());
            Assert.assertEquals(5, this.removedEdges.size());
        }
    }

    @Test
    public void dropMultiplePathsToVerticesWithHasWithUserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        bVertexLabel.ensureEdgeLabelExist(
                "bc",
                cVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "c3");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "c4");
        Vertex c5 = this.sqlgGraph.addVertex(T.label, "C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "c5");
        a1.addEdge("ab", b1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        a1.addEdge("ab", b2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b1.addEdge("bc", c1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b1.addEdge("bc", c2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b1.addEdge("bc", c3, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b2.addEdge("bc", c3, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b2.addEdge("bc", c4, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        b2.addEdge("bc", c5, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A")
                .out()
                .out().has("name", P.within("c1", "c2", "c3", "c4"))
                .toList();
        Assert.assertEquals(5, vertices.size());
        Assert.assertTrue(vertices.removeAll(Arrays.asList(c1, c2, c3, c4)));
        Assert.assertEquals(0, vertices.size());

        this.dropTraversal.V().hasLabel("A")
                .out()
                .out().has("name", P.within("c1", "c2", "c3", "c4"))
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1L, this.sqlgGraph.traversal().V().hasLabel("C").count().next(), 0L);
        Assert.assertEquals(1L, this.sqlgGraph.traversal().E().hasLabel("bc").count().next(), 0L);

        if (this.mutatingCallback) {
            Assert.assertEquals(4, this.removedVertices.size());
            Assert.assertEquals(5, this.removedEdges.size());
        }
    }

    @Test
    public void testDropStepWithJoinVertexStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Edge e1 = a1.addEdge("ab", b1);
        Edge e2 = a1.addEdge("ab", b2);
        Edge e3 = a1.addEdge("ab", b3);
        this.sqlgGraph.tx().commit();

        this.dropTraversal
                .V().hasLabel("A")
                .local(
                        __.out("ab").has("name", "b2").drop()
                )
                .iterate();
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").out("ab").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(b1, b3)));
        vertices = this.sqlgGraph.traversal().V().hasLabel("B").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(b1, b3)));
        List<Edge> edges = this.sqlgGraph.traversal().E().hasLabel("ab").toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertTrue(edges.containsAll(Arrays.asList(e1, e3)));
        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertEquals(1, this.removedEdges.size());
        }
    }

    @Test
    public void testDropStepWithJoinVertexStepUserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a3");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b3");
        Edge e1 = a1.addEdge("ab", b1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Edge e2 = a1.addEdge("ab", b2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Edge e3 = a1.addEdge("ab", b3, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        this.dropTraversal
                .V().hasLabel("A")
                .local(
                        __.out("ab").has("name", "b2").drop()
                )
                .iterate();
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").out("ab").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(b1, b3)));
        vertices = this.sqlgGraph.traversal().V().hasLabel("B").toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(b1, b3)));
        List<Edge> edges = this.sqlgGraph.traversal().E().hasLabel("ab").toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertTrue(edges.containsAll(Arrays.asList(e1, e3)));
        if (this.mutatingCallback) {
            Assert.assertEquals(1, this.removedVertices.size());
            Assert.assertEquals(1, this.removedEdges.size());
        }
    }

    @Test
    public void testDropOutMultiple() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("e1", b1);

        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        a2.addEdge("e1", c2);

        this.sqlgGraph.tx().commit();

        this.dropTraversal.V(a1, a2).out("e1").drop().iterate();
        this.dropTraversal.V(a1, a2).drop().iterate();
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("B").toList();
        Assert.assertEquals(0, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("C").toList();
        Assert.assertEquals(0, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(0, vertices.size());

        if (this.mutatingCallback) {
            Assert.assertEquals(4, this.removedVertices.size());
            Assert.assertTrue(this.removedVertices.contains(b1));
            Assert.assertTrue(this.removedVertices.contains(c2));
            Assert.assertTrue(this.removedVertices.contains(a1));
            Assert.assertTrue(this.removedVertices.contains(a2));
        }
    }

    @Test
    public void testDropOutMultipleOneUserSuppliedId() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid1"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid1"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid1"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "e1",
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid1"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "e1",
                cVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid1"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid1", "a1", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid1", "b1", "name", "b1");
        a1.addEdge("e1", b1, "uid1", "e1");

        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "uid1", "a2", "name", "a2");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "uid1", "c1", "name", "c2");
        a2.addEdge("e1", c2, "uid1", "e2");

        this.sqlgGraph.tx().commit();

        this.dropTraversal.V(a1, a2).out("e1").drop().iterate();
        this.dropTraversal.V(a1, a2).drop().iterate();
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("B").toList();
        Assert.assertEquals(0, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("C").toList();
        Assert.assertEquals(0, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(0, vertices.size());

        if (this.mutatingCallback) {
            Assert.assertEquals(4, this.removedVertices.size());
            Assert.assertTrue(this.removedVertices.contains(b1));
            Assert.assertTrue(this.removedVertices.contains(c2));
            Assert.assertTrue(this.removedVertices.contains(a1));
            Assert.assertTrue(this.removedVertices.contains(a2));
        }
    }

    @Test
    public void testDropOutMultipleUserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "e1",
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "e1",
                cVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid1", "a1", "uid2", "a11", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid1", "b1", "uid2", "b11", "name", "b1");
        a1.addEdge("e1", b1, "uid1", "e1", "uid2", "e11");

        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "uid1", "a2", "uid2", "a22", "name", "a2");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "uid1", "c1", "uid2", "c11", "name", "c2");
        a2.addEdge("e1", c2, "uid1", "e2", "uid2", "e22");

        this.sqlgGraph.tx().commit();

        this.dropTraversal.V(a1, a2).out("e1").drop().iterate();
        this.dropTraversal.V(a1, a2).drop().iterate();
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("B").toList();
        Assert.assertEquals(0, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("C").toList();
        Assert.assertEquals(0, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(0, vertices.size());

        if (this.mutatingCallback) {
            Assert.assertEquals(4, this.removedVertices.size());
            Assert.assertTrue(this.removedVertices.contains(b1));
            Assert.assertTrue(this.removedVertices.contains(c2));
            Assert.assertTrue(this.removedVertices.contains(a1));
            Assert.assertTrue(this.removedVertices.contains(a2));
        }
    }

    @Test
    public void testDropOutMultipleUserSuppliedIdsSimpler() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "e1",
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "e1",
                cVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid1", "a1", "uid2", "a11", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid1", "b1", "uid2", "b11", "name", "b1");
        a1.addEdge("e1", b1, "uid1", "e1", "uid2", "e11");

        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "uid1", "a2", "uid2", "a22", "name", "a2");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "uid1", "c1", "uid2", "c11", "name", "c2");
        a2.addEdge("e1", c2, "uid1", "e2", "uid2", "e22");

        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V(a1, a2).out("e1").toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testDropOutMultipleAcrossSchemas() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "name", "b1");
        a1.addEdge("e1", b1);

        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "a2");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C.C", "name", "c2");
        a2.addEdge("e1", c2);

        this.sqlgGraph.tx().commit();

        this.dropTraversal.V(a1, a2).out("e1").drop().iterate();
        this.dropTraversal.V(a1, a2).drop().iterate();
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("B").toList();
        Assert.assertEquals(0, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("C").toList();
        Assert.assertEquals(0, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(0, vertices.size());

        if (this.mutatingCallback) {
            Assert.assertEquals(4, this.removedVertices.size());
            Assert.assertTrue(this.removedVertices.contains(b1));
            Assert.assertTrue(this.removedVertices.contains(c2));
            Assert.assertTrue(this.removedVertices.contains(a1));
            Assert.assertTrue(this.removedVertices.contains(a2));
        }
    }

    @Test
    public void testDropOutMultipleAcrossSchemasuserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "e1",
                bVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "e1",
                cVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "b1");
        a1.addEdge("e1", b1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());

        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A.A", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "a2");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C.C", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "c2");
        a2.addEdge("e1", c2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());

        this.sqlgGraph.tx().commit();

        this.dropTraversal.V(a1, a2).out("e1").drop().iterate();
        this.dropTraversal.V(a1, a2).drop().iterate();
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("B").toList();
        Assert.assertEquals(0, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("C").toList();
        Assert.assertEquals(0, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(0, vertices.size());

        if (this.mutatingCallback) {
            Assert.assertEquals(4, this.removedVertices.size());
            Assert.assertTrue(this.removedVertices.contains(b1));
            Assert.assertTrue(this.removedVertices.contains(c2));
            Assert.assertTrue(this.removedVertices.contains(a1));
            Assert.assertTrue(this.removedVertices.contains(a2));
        }
    }

    @Test
    public void testOptionalAndDrop() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        b2.addEdge("bc", c1);
        b2.addEdge("bc", c2);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.traversal().V().hasLabel("A").out().optional(__.out()).drop().iterate();
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(a1));
        Assert.assertTrue(vertices.contains(b2));
    }

    static abstract class AbstractMutationListener implements MutationListener {
        @Override
        public void vertexAdded(final Vertex vertex) {

        }

        @Override
        public void vertexRemoved(final Vertex vertex) {

        }

        @Override
        public void vertexPropertyChanged(Vertex element, VertexProperty oldValue,
                                          Object setValue, Object... vertexPropertyKeyValues) {

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
