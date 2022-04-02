package org.umlg.sqlg.test.topology.propertydefinition;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;

public class TestPropertyCheckConstraint extends BaseTest {

    @Test
    public void testVertexLabelCheckConstraint() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("A", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.from(1, 1), null, "(name <> 'a')"));
        }});
        this.sqlgGraph.tx().commit();
        boolean failure = false;
        try {
            this.sqlgGraph.addVertex(T.label, "A", "name", "a");
            this.sqlgGraph.tx().commit();
        } catch (Exception e) {
            failure = true;
            this.sqlgGraph.tx().rollback();
        }
        Assert.assertTrue(failure);
        failure = false;
        try {
            this.sqlgGraph.addVertex(T.label, "A", "name", "b");
            this.sqlgGraph.tx().commit();
        } catch (Exception e) {
            failure = true;
            this.sqlgGraph.tx().rollback();
        }
        Assert.assertFalse(failure);
    }

    @Test
    public void testEdgeLabelCheckConstraint() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.from(1, 1), null, "(name <> 'a')"));
        }});
        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.from(1, 1), null, "(name <> 'a')"));
        }});
        aVertexLabel.ensureEdgeLabelExist("ab",bVertexLabel, new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.from(1, 1), null, "(name <> 'a')"));
        }});
        this.sqlgGraph.tx().commit();
        boolean failure = false;
        try {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "b");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
            a.addEdge("ab", b, "name", "a");
            this.sqlgGraph.tx().commit();
        } catch (Exception e) {
            failure = true;
            this.sqlgGraph.tx().rollback();
        }
        Assert.assertTrue(failure);
        failure = false;
        try {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "b");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
            a.addEdge("ab", b, "name", "b");
            this.sqlgGraph.tx().commit();
        } catch (Exception e) {
            failure = true;
            this.sqlgGraph.tx().rollback();
        }
        Assert.assertFalse(failure);
    }
}
