package org.umlg.sqlg.test.doctests;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Index;
import org.umlg.sqlg.structure.topology.IndexType;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2014/12/14
 * Time: 3:29 PM
 */
@SuppressWarnings("Duplicates")
public class TestForDocs extends BaseTest {

    @Test
    public void testIndex() {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person",
                new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
        }});
        Optional<PropertyColumn> namePropertyOptional = personVertexLabel.getProperty("name");
        assertTrue(namePropertyOptional.isPresent());
        Index index = personVertexLabel.ensureIndexExists(IndexType.NON_UNIQUE, Collections.singletonList(namePropertyOptional.get()));
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
        List<Vertex> johns = this.sqlgGraph.traversal().V()
                .hasLabel("Person")
                .has("name", "John")
                .toList();

        assertEquals(1, johns.size());
    }

}
