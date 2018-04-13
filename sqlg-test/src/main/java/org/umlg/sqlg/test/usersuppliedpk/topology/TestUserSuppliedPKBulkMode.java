package org.umlg.sqlg.test.usersuppliedpk.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
}
