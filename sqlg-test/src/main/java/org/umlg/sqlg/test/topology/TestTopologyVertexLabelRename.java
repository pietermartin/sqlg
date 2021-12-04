package org.umlg.sqlg.test.topology;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.TopologyChangeAction;
import org.umlg.sqlg.structure.TopologyInf;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.EdgeRole;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class TestTopologyVertexLabelRename extends BaseTest {

    private final List<Triple<TopologyInf, TopologyInf, TopologyChangeAction>> topologyListenerTriple = new ArrayList<>();

    @Before
    public void before() throws Exception {
        super.before();
        this.topologyListenerTriple.clear();
    }

    @Test
    public void testVertexLabelSimple() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<>() {{
            put("a", PropertyType.STRING);
        }});
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow();
        Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").isPresent());
        Assert.assertFalse(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").isPresent());
        Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getProperty("a").isPresent());
        aVertexLabel.rename("B");
        Assert.assertFalse(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").isPresent());
        Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").isPresent());
        Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getProperty("a").isPresent());
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").isPresent());
        Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").isPresent());
        Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getProperty("a").isPresent());

        Assert.assertEquals(2, this.topologyListenerTriple.size());
        Assert.assertEquals(TopologyChangeAction.CREATE, this.topologyListenerTriple.get(0).getRight());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(1).getRight());
        Assert.assertEquals("A", this.topologyListenerTriple.get(1).getMiddle().getName());
        Assert.assertEquals("B", this.topologyListenerTriple.get(1).getLeft().getName());
    }

    @Test
    public void testVertexLabelSimpleWithQueries() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<>() {{
            put("a", PropertyType.STRING);
        }});
        this.sqlgGraph.addVertex(T.label, "A", "a", "halo1");
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow();
        Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").isPresent());
        Assert.assertFalse(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").isPresent());
        aVertexLabel.rename("B");
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("B").count().next(), 0);

        Assert.assertFalse(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").isPresent());
        Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").isPresent());
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").isPresent());
        Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").isPresent());
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("B").count().next(), 0);
    }

    @Test
    public void testVertexLabelRenameAsEdgeRole() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<>() {{
            put("a", PropertyType.STRING);
        }});
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B", new HashMap<>() {{
            put("a", PropertyType.STRING);
        }});
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel);
        this.sqlgGraph.tx().commit();

        EdgeLabel edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
        Set<VertexLabel> outVertexLabels = edgeLabel.getOutVertexLabels();
        Assert.assertEquals(1, outVertexLabels.size());
        Assert.assertEquals("A", new ArrayList<>(outVertexLabels).get(0).getLabel());
        Set<VertexLabel> inVertexLabels = edgeLabel.getInVertexLabels();
        Assert.assertEquals(1, inVertexLabels.size());
        Assert.assertEquals("B", new ArrayList<>(inVertexLabels).get(0).getLabel());
        Set<EdgeRole> inEdgeRoles = edgeLabel.getInEdgeRoles();
        Assert.assertEquals(1, inEdgeRoles.size());
        Set<EdgeRole> outEdgeRoles = edgeLabel.getOutEdgeRoles();
        Assert.assertEquals(1, outEdgeRoles.size());

        List<Vertex> outEdges = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "A")
                .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                .toList();
        Assert.assertEquals(1, outEdges.size());
        List<Vertex> inEdges = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "A")
                .out(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
                .toList();
        Assert.assertEquals(0, inEdges.size());
        outEdges = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "B")
                .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                .toList();
        Assert.assertEquals(0, outEdges.size());
        inEdges = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "B")
                .out(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
                .toList();
        Assert.assertEquals(1, inEdges.size());

        aVertexLabel.rename("AA");

        //before commit
        edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
        outVertexLabels = edgeLabel.getOutVertexLabels();
        Assert.assertEquals(1, outVertexLabels.size());
        Assert.assertEquals("AA", new ArrayList<>(outVertexLabels).get(0).getLabel());
        inVertexLabels = edgeLabel.getInVertexLabels();
        Assert.assertEquals(1, inVertexLabels.size());
        Assert.assertEquals("B", new ArrayList<>(inVertexLabels).get(0).getLabel());
        inEdgeRoles = edgeLabel.getInEdgeRoles();
        Assert.assertEquals(1, inEdgeRoles.size());
        outEdgeRoles = edgeLabel.getOutEdgeRoles();
        Assert.assertEquals(1, outEdgeRoles.size());

        this.sqlgGraph.tx().commit();

        edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
        outVertexLabels = edgeLabel.getOutVertexLabels();
        Assert.assertEquals(1, outVertexLabels.size());
        Assert.assertEquals("AA", new ArrayList<>(outVertexLabels).get(0).getLabel());
        inVertexLabels = edgeLabel.getInVertexLabels();
        Assert.assertEquals(1, inVertexLabels.size());
        Assert.assertEquals("B", new ArrayList<>(inVertexLabels).get(0).getLabel());
        inEdgeRoles = edgeLabel.getInEdgeRoles();
        Assert.assertEquals(1, inEdgeRoles.size());
        outEdgeRoles = edgeLabel.getOutEdgeRoles();
        Assert.assertEquals(1, outEdgeRoles.size());

        outEdges = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "AA")
                .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                .toList();
        Assert.assertEquals(1, outEdges.size());
        inEdges = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "AA")
                .out(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
                .toList();
        Assert.assertEquals(0, inEdges.size());
        outEdges = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "B")
                .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                .toList();
        Assert.assertEquals(0, outEdges.size());
        inEdges = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "B")
                .out(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
                .toList();
        Assert.assertEquals(1, inEdges.size());
    }
}
