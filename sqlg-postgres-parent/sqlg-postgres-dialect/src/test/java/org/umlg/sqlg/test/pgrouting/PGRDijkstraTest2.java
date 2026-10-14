package org.umlg.sqlg.test.pgrouting;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.topology.VertexLabel;

import java.util.LinkedHashMap;
import java.util.List;

public class PGRDijkstraTest2 extends BasePGRouting {

    @Test
    public void test() {

        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
        }});
        aVertexLabel.ensureEdgeLabelExist("ab", aVertexLabel, new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("cost", PropertyDefinition.of(PropertyType.DOUBLE));
            put("reverse_cost", PropertyDefinition.of(PropertyType.DOUBLE));
        }});
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");

        String edgeLabelName = "edges";
        Edge e1 = a1.addEdge(edgeLabelName, a2, "cost", 1D, "reverse_cost", 1D);

        this.sqlgGraph.tx().commit();

        List<Path> directed_paths = this.sqlgGraph.traversal().E().hasLabel("edges")
                .dijkstra(((RecordId)(a1.id())).sequenceId(), ((RecordId)(a2.id())).sequenceId(), true)
                .toList();
        Assert.assertEquals(1, directed_paths.size());
        Assert.assertEquals(3, directed_paths.get(0).size());


    }

}
