package org.umlg.sqlg.test.usersuppliedpk.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/03/31
 */
public class TestUserSuppliedPKBulkMode extends BaseTest {

    @Test
    public void testVertexLabelUserSuppliedBulkMode() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new HashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("A").toList().size());
    }

    @Test
    public void testVertexAndEdgeLabelUserSuppliedBulkMode() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new HashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new HashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name"))
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 100; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a" + i);
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b" + i);
            a1.addEdge("ab", b1);
        }
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("A").out().toList().size());
    }

    @Test
    public void testVertexBatchStreamMode() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStreamingBatchMode());
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new HashMap<String, PropertyType>() {{
                    put("name1", PropertyType.STRING);
                    put("name2", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name1", "name2"))
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex(T.label, "A", "name1", "a" + i, "name2", "a" + i);
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("A").toList().size());
    }

    @Test
    public void testEdgeBatchStreamMode() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStreamingBatchMode());
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new HashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new HashMap<String, PropertyType>() {{
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name"))
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel);
        this.sqlgGraph.tx().commit();
        SqlgVertex a1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        Map<String, Vertex> cache = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            cache.put("b" + i,  this.sqlgGraph.addVertex(T.label, "B", "name", "b" + i));
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (String key : cache.keySet()) {
            SqlgVertex sqlgVertex = (SqlgVertex) cache.get(key);
            a1.streamEdge("ab", sqlgVertex);
        }
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("A").out().toList().size());
    }

    @Test
    public void testSharding2() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        this.sqlgGraph.tx().commit();

        List<String> tenantIds = Arrays.asList("RNC1", "RNC2", "RNC3", "RNC4");
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Edge> edges = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            int j = i % 4;
            String tenantId = tenantIds.get(j);
            Vertex a = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "dist", tenantId, "value", Integer.toString(i));
            Vertex b = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "dist", tenantId, "value", Integer.toString(i));
            Edge e = a.addEdge("ab", b, "uid", UUID.randomUUID().toString(), "dist", tenantId, "value", Integer.toString(i));
            edges.add(e);
        }
        this.sqlgGraph.tx().commit();
        for (Edge edge : edges) {
            Edge other = this.sqlgGraph.traversal().E(edge.id()).next();
            Assert.assertEquals(edge, other);
        }
    }
}
