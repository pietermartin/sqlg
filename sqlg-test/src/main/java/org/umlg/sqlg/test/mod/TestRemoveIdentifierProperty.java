package org.umlg.sqlg.test.mod;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.LinkedHashMap;
import java.util.List;

public class TestRemoveIdentifierProperty extends BaseTest {

    @Test
    public void testRemoveIdentifierPropertyFromVertexLabel() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                            put("id1", PropertyDefinition.of(PropertyType.varChar(10)));
                            put("id2", PropertyDefinition.of(PropertyType.varChar(10)));
                        }},
                        ListOrderedSet.listOrderedSet(List.of("id1", "id2"))
                );
        this.sqlgGraph.tx().commit();
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow();
        PropertyColumn propertyColumn = aVertexLabel.getProperty("id1").orElseThrow();
        try {
            propertyColumn.remove();
            Assert.fail("Identifier properties are not allowed to be remove.");
        } catch (IllegalStateException e) {
            Assert.assertEquals("Identifier column 'id1' may not be removed.", e.getMessage());
        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testRemoveIdentifierPropertyFromEdgeLabel() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B");
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new LinkedHashMap<>() {{
                    put("id1", PropertyDefinition.of(PropertyType.varChar(10)));
                    put("id2", PropertyDefinition.of(PropertyType.varChar(10)));
                }},
                ListOrderedSet.listOrderedSet(List.of("id1", "id2"))
        );
        this.sqlgGraph.tx().commit();

        EdgeLabel abEdgeLabel = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
        PropertyColumn propertyColumn = abEdgeLabel.getProperty("id1").orElseThrow();
        try {
            propertyColumn.remove();
            Assert.fail("Identifier properties are not allowed to be remove.");
        } catch (IllegalStateException e) {
            Assert.assertEquals("Identifier column 'id1' may not be removed.", e.getMessage());
        }
        this.sqlgGraph.tx().rollback();
    }
}
