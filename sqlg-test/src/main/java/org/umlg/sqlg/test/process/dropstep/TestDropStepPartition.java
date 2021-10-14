package org.umlg.sqlg.test.process.dropstep;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
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
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.topology.IndexType;
import org.umlg.sqlg.structure.topology.PartitionType;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/11/11
 */
@RunWith(Parameterized.class)
public class TestDropStepPartition extends BaseTest {

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
//        return Collections.singletonList(new Object[]{Boolean.TRUE, Boolean.FALSE});
//        return Collections.singletonList(new Object[]{Boolean.TRUE, Boolean.TRUE});
    }


    @Before
    public void before() throws Exception {
        super.before();
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsPartitioning());
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
        VertexLabel policyDiscrepancyVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "PolicyDiscrepancy",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("part", PropertyType.STRING);
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2", "part")),
               PartitionType.LIST,
               "part"
        );
        policyDiscrepancyVertexLabel.ensureListPartitionExists("part1", "'part1'");
        policyDiscrepancyVertexLabel.ensureListPartitionExists("part2", "'part2'");

        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "PolicyDiscrepancy",
                    "uid1", "uid1" + i,
                    "uid2", "uid2" + i,
                    "name", "name" + i,
                    "part", (i & 1) == 0 ? "part1" : "part2"
            );
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("PolicyDiscrepancy").toList().size(), 0);

        sqlgGraph.traversal().V()
                .hasLabel("PolicyDiscrepancy")
                .has("uid1", "uid10")
                .has("uid2", "uid20")
                .has("part", "part1")
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

        sqlgGraph.traversal().V()
                .hasLabel("PolicyDiscrepancy")
                .has("part", "part1")
                .drop().iterate();
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(3, this.sqlgGraph.traversal().V().hasLabel("PolicyDiscrepancy").toList().size(), 0);
    }

//    @Test
    public void testNormalAndUserSuppliedIdsInPerformance() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid")),
                PartitionType.LIST,
                "name"
        );
        aVertexLabel.ensureListPartitionExists("a1", "'name1'");
        aVertexLabel.ensureListPartitionExists("a2", "'name2'");
        PropertyColumn propertyColumn = aVertexLabel.getProperty("uid").orElseThrow(RuntimeException::new);
        aVertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumn));
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "B",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid")),
                PartitionType.LIST,
                "\"name\""
        );
        bVertexLabel.ensureListPartitionExists("b1", "'name1'");
        bVertexLabel.ensureListPartitionExists("b2", "'name2'");
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 1_000_000; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid", "uid" + i, "name", "name" + (i % 2 == 0 ? "1" : "2"));
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid", "uid" + i, "name", "name" + (i % 2 == 0 ? "1" : "2"));
            Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "uid", "uidx" + i, "name", "name" + (i % 2 == 0 ? "1" : "2"));
            a1.addEdge("ab", b1, "uid", "uid" + i);
            a1.addEdge("ab", b2, "uid", "uidx" + i);
        }
        this.sqlgGraph.tx().commit();

        List<Vertex> aVertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(1_000_000, aVertices.size(), 0);
        Assert.assertEquals(2_000_000, this.sqlgGraph.traversal().V().hasLabel("A").out().toList().size(), 0);
        Assert.assertEquals(2_000_000, this.sqlgGraph.traversal().V().hasLabel("B").toList().size(), 0);

        List<Vertex> name1Vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", "name1").toList();
        Assert.assertEquals(500_000, name1Vertices.size());
        List<Vertex> name2Vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", "name2").toList();
        Assert.assertEquals(500_000, name2Vertices.size());

        List<List<Vertex>> partitionedAVertices = ListUtils.partition(name1Vertices, 10);
        List<String> uids = new ArrayList<>();
        for (Vertex vertex: partitionedAVertices.get(0)) {
            uids.add(vertex.value("uid"));
        }

        this.sqlgGraph.traversal().V()
                .hasLabel("A")
                .has("name", P.within("name1"))
                .has("uid", P.within(uids))
                .drop().iterate();
        this.sqlgGraph.tx().commit();
//
//        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("B").toList().size(), 0);
    }

    @Test
    public void testNormalAndUserSuppliedIdsIn() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "B",
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name")),
                PartitionType.LIST,
                "\"name\""
        );
        bVertexLabel.ensureListPartitionExists("1", "'name1'");
        bVertexLabel.ensureListPartitionExists("2", "'name2'");
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<String, PropertyType>() {{
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
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name")),
                PartitionType.LIST,
                "name"
        );
        aVertexLabel.ensureListPartitionExists("part1", "'name1'");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new LinkedHashMap<String, PropertyType>() {{
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
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("name", PropertyType.STRING);
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2", "name")),
                PartitionType.LIST,
                "name"
        );
        vertexLabel.ensureListPartitionExists("part1", "'a1'");
        vertexLabel.ensureListPartitionExists("part2", "'a2'");
        vertexLabel.ensureListPartitionExists("part3", "'a3'");
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
                .hasId(RecordId.from(sqlgGraph, schemaTable.getSchema() + "." + schemaTable.getTable() + RecordId.RECORD_ID_DELIMITER + "[" + uid1 + "," + uid2 + ",a1" + "]"))
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
    public void testDropStep() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("name", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "name")),
                PartitionType.LIST,
                "name"
        );
        vertexLabel.ensureListPartitionExists("part1", "'a1'");
        vertexLabel.ensureListPartitionExists("part2", "'a2'");
        vertexLabel.ensureListPartitionExists("part3", "'a3'");
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(), "name", "a3");
        this.sqlgGraph.tx().commit();

        this.dropTraversal.V().hasLabel("A").has("name", "a1").drop().hasNext();
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
    public void testDropStepWithJoinWithuserSuppliedIds() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2", "name")),
                PartitionType.LIST,
                "name"
        );
        aVertexLabel.ensureListPartitionExists("parta1", "'a1'");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2", "name")),
                PartitionType.LIST,
                "name"
        );
        bVertexLabel.ensureListPartitionExists("partb1", "'b1'");
        bVertexLabel.ensureListPartitionExists("partb2", "'b2'");
        bVertexLabel.ensureListPartitionExists("partb3", "'b3'");
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2", "name")),
                PartitionType.LIST,
                "name"
        );
        cVertexLabel.ensureListPartitionExists("partc1", "'c1'");
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
