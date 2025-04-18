package org.umlg.sqlg.test.pgrouting;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.LinkedHashMap;

public class TestPGRouting extends BaseTest {

    @Test
    public void testPGRouting() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("wiki", new LinkedHashMap<>() {{
                    put("source", PropertyDefinition.of(PropertyType.INTEGER));
                    put("target", PropertyDefinition.of(PropertyType.INTEGER));
                    put("cost", PropertyDefinition.of(PropertyType.INTEGER));
                }});
        this.sqlgGraph.tx().commit();

//        INSERT INTO wiki (source, target, cost) VALUES
//        (1, 2, 7),  (1, 3, 9), (1, 6, 14),
//        (2, 3, 10), (2, 4, 15),
//        (3, 6, 2),  (3, 4, 11),
//        (4, 5, 6),
//        (5, 6, 9);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "wiki", "source", 1, "target", 2, "cost", 7);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "wiki", "source", 1, "target", 3, "cost", 9);
        this.sqlgGraph.addVertex(T.label, "wiki", "source", 1, "target", 6, "cost", 14);
        this.sqlgGraph.addVertex(T.label, "wiki", "source", 2, "target", 3, "cost", 10);
        this.sqlgGraph.addVertex(T.label, "wiki", "source", 2, "target", 4, "cost", 15);
        this.sqlgGraph.addVertex(T.label, "wiki", "source", 3, "target", 6, "cost", 2);
        this.sqlgGraph.addVertex(T.label, "wiki", "source", 3, "target", 4, "cost", 11);
        this.sqlgGraph.addVertex(T.label, "wiki", "source", 4, "target", 5, "cost", 6);
        this.sqlgGraph.addVertex(T.label, "wiki", "source", 5, "target", 6, "cost", 9);
        this.sqlgGraph.tx().commit();

//        this.sqlgGraph.traversal().V().hasLabel("wiki")
//                .pgr_dijkstra(1, 5)
//                .path();
    }

    @Test
    public void testPGRouting2() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();

        VertexLabel node = publicSchema.ensureVertexLabelExist("Node",  new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.INTEGER));
        }});
        node.ensureEdgeLabelExist("to",  node,  new LinkedHashMap<>() {{
            put("cost", PropertyDefinition.of(PropertyType.INTEGER));
        }});
        this.sqlgGraph.tx().commit();

        Vertex _1 = this.sqlgGraph.addVertex(T.label, "Node", "name", 1);
        Vertex _2 = this.sqlgGraph.addVertex(T.label, "Node", "name", 2);
        Vertex _3 = this.sqlgGraph.addVertex(T.label, "Node", "name", 3);
        Vertex _4 = this.sqlgGraph.addVertex(T.label, "Node", "name", 4);
        Vertex _5 = this.sqlgGraph.addVertex(T.label, "Node", "name", 5);
        Vertex _6 = this.sqlgGraph.addVertex(T.label, "Node", "name", 6);
        _1.addEdge("to", _2, "cost", 7);
        _1.addEdge("to", _3, "cost", 9);
        _1.addEdge("to", _6, "cost", 14);
        _2.addEdge("to", _4, "cost", 13);
        _2.addEdge("to", _3, "cost", 10);
        _3.addEdge("to", _6, "cost", 2);
        _3.addEdge("to", _4, "cost", 11);
        _6.addEdge("to", _5, "cost", 9);
        _5.addEdge("to", _4, "cost", 6);
        this.sqlgGraph.tx().commit();
    }
}
