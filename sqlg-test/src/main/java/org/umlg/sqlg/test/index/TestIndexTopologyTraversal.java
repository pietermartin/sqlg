package org.umlg.sqlg.test.index;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/11/26
 * Time: 11:01 PM
 */
public class TestIndexTopologyTraversal extends BaseTest {

    @Test
    public void testIndexTopologyTraversal() {
        Schema schemaA = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel aVertexLabel = schemaA.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = schemaA.ensureVertexLabelExist("B");
        Map<String, PropertyDefinition> properties = new HashMap<>();
        properties.put("name", PropertyDefinition.of(PropertyType.STRING));
        EdgeLabel edgeLabel = this.sqlgGraph.getTopology().ensureEdgeLabelExist("ab", aVertexLabel, bVertexLabel, properties);
        edgeLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(edgeLabel.getProperty("name").get()));
        this.sqlgGraph.tx().commit();

        List<Vertex> indexes = this.sqlgGraph.topology().V().hasLabel("sqlg_schema.index").toList();
        assertEquals(1, indexes.size());

        List<Vertex> indexProperties = this.sqlgGraph.topology().V()
                .hasLabel("sqlg_schema.schema").has("name", "A")
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                .out(Topology.SQLG_SCHEMA_EDGE_INDEX_EDGE)
                .out(Topology.SQLG_SCHEMA_INDEX_PROPERTY_EDGE)
                .toList();
        assertEquals(1, indexProperties.size());
    }
}
