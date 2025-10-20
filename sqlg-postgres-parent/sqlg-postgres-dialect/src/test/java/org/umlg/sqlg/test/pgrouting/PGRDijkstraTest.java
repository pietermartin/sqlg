package org.umlg.sqlg.test.pgrouting;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.topology.VertexLabel;

import java.util.LinkedHashMap;
import java.util.List;

public class PGRDijkstraTest extends BasePGRouting {

    private final static Logger LOGGER = LoggerFactory.getLogger(PGRDijkstraTest.class);

    @Test
    public void testDirectionFalseIsNotTheSameAsDirectionBoth() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
        }});
        aVertexLabel.ensureEdgeLabelExist("ab", aVertexLabel, new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("cost", PropertyDefinition.of(PropertyType.DOUBLE));
            put("reverse_cost", PropertyDefinition.of(PropertyType.DOUBLE));
        }});
        this.sqlgGraph.tx().commit();


        Vertex v6 = this.sqlgGraph.addVertex(T.label, "A", "name", "6");
        Vertex v7 = this.sqlgGraph.addVertex(T.label, "A", "name", "7");
        Vertex v11 = this.sqlgGraph.addVertex(T.label, "A", "name", "11");
        Vertex v16 = this.sqlgGraph.addVertex(T.label, "A", "name", "16");
        Vertex v15 = this.sqlgGraph.addVertex(T.label, "A", "name", "15");
        Vertex v10 = this.sqlgGraph.addVertex(T.label, "A", "name", "10");

        String edgeLabelName = "edges";
        Edge e4 = v6.addEdge(edgeLabelName, v7, "source", 6L, "target", 7L, "cost", 1D, "reverse_cost", 1D);
        Edge e8 = v7.addEdge(edgeLabelName, v11, "source", 7L, "target", 11L, "cost", 1D, "reverse_cost", 1D);
        Edge e9 = v11.addEdge(edgeLabelName, v16, "source", 11L, "target", 16L, "cost", 1D, "reverse_cost", 1D);
        Edge e16 = v15.addEdge(edgeLabelName, v16, "source", 15L, "target", 16L, "cost", 1D, "reverse_cost", 1D);
        Edge e3 = v10.addEdge(edgeLabelName, v15, "source", 10L, "target", 15L, "cost", -1D, "reverse_cost", 1D);
        Edge e2 = v6.addEdge(edgeLabelName, v10, "source", 6L, "target", 10L, "cost", -1D, "reverse_cost", 1D);

        this.sqlgGraph.tx().commit();

        List<Path> directed_paths = this.sqlgGraph.traversal().E().hasLabel("edges")
                .dijkstra(((RecordId)(v6.id())).sequenceId(), ((RecordId)(v10.id())).sequenceId(), true)
                .toList();
        Assert.assertEquals(1, directed_paths.size());
        Assert.assertEquals(11, directed_paths.get(0).size());

        List<Path> undirected_paths = this.sqlgGraph.traversal().E().hasLabel("edges")
                .dijkstra(((RecordId)(v6.id())).sequenceId(), ((RecordId)(v10.id())).sequenceId(), false)
                .toList();
        Assert.assertEquals(1, undirected_paths.size());
        Assert.assertEquals(3, undirected_paths.get(0).size());

    }

    @Test
    public void testDirection() {

        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
        }});
        aVertexLabel.ensureEdgeLabelExist("ab", aVertexLabel, new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("cost", PropertyDefinition.of(PropertyType.DOUBLE));
            put("reverse_cost", PropertyDefinition.of(PropertyType.DOUBLE));
        }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().lock();

        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A", "name", "b");
        Vertex vertex3 = this.sqlgGraph.addVertex(T.label, "A", "name", "c");
        vertex1.addEdge("ab", vertex2, "cost", 1D, "reverse_cost", 1D);
        vertex2.addEdge("ab", vertex3, "cost", 1D, "reverse_cost", 1D);
        vertex1.addEdge("ab", vertex3, "cost", -1D, "reverse_cost", 1D);
        this.sqlgGraph.tx().commit();

        RecordId recordId1 = (RecordId) vertex1.id();
        RecordId recordId2 = (RecordId) vertex2.id();
        RecordId recordId3 = (RecordId) vertex3.id();
        List<Path> directed_paths = this.sqlgGraph.traversal().E().hasLabel("ab")
                .dijkstra(recordId1.sequenceId(), List.of(recordId3.sequenceId()), true)
                .toList();
        Assert.assertEquals(1, directed_paths.size());
        Assert.assertEquals(5, directed_paths.get(0).size());

        List<Path> undirected_paths = this.sqlgGraph.traversal().E().hasLabel("ab")
                .dijkstra(recordId1.sequenceId(), List.of(recordId3.sequenceId()), false)
                .toList();
        Assert.assertEquals(1, undirected_paths.size());
        Assert.assertEquals(3, undirected_paths.get(0).size());
    }

    @Test
    public void testDirectionWithoutReverseCost() {

        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
        }});
        aVertexLabel.ensureEdgeLabelExist("ab", aVertexLabel, new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("cost", PropertyDefinition.of(PropertyType.DOUBLE));
        }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().lock();

        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A", "name", "b");
        Vertex vertex3 = this.sqlgGraph.addVertex(T.label, "A", "name", "c");
        vertex1.addEdge("ab", vertex2, "cost", 1D);
        vertex2.addEdge("ab", vertex3, "cost", 1D);
        vertex1.addEdge("ab", vertex3, "cost", -1D);
        this.sqlgGraph.tx().commit();

        RecordId recordId1 = (RecordId) vertex1.id();
        RecordId recordId2 = (RecordId) vertex2.id();
        RecordId recordId3 = (RecordId) vertex3.id();
        List<Path> directed_paths = this.sqlgGraph.traversal().E().hasLabel("ab")
                .dijkstra(recordId1.sequenceId(), List.of(recordId3.sequenceId()), true)
                .toList();
        Assert.assertEquals(1, directed_paths.size());
        Assert.assertEquals(5, directed_paths.get(0).size());

        List<Path> undirected_paths = this.sqlgGraph.traversal().E().hasLabel("ab")
                .dijkstra(recordId1.sequenceId(), List.of(recordId3.sequenceId()), false)
                .toList();
        Assert.assertEquals(1, directed_paths.size());
        Assert.assertEquals(5, directed_paths.get(0).size());
    }
}
