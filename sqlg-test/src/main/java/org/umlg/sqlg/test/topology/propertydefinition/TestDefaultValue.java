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
import java.util.List;

public class TestDefaultValue extends BaseTest  {

    @Test
    public void testVertexLabelDefaultValue() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("A", new HashMap<>() {{
            put("a", PropertyDefinition.of(PropertyType.STRING, Multiplicity.from(1, 1), "'aaa'"));
        }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        List<String> values = this.sqlgGraph.traversal().V().hasLabel("A").<String>values("a").toList();
        Assert.assertEquals(1, values.size());
        Assert.assertEquals("aaa", values.get(0));
    }

    @Test
    public void testEdgeLabelDefaultValue() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A", new HashMap<>() {{
            put("a", PropertyDefinition.of(PropertyType.STRING, Multiplicity.from(1, 1), "'aaa'"));
        }});
        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B", new HashMap<>() {{
            put("a", PropertyDefinition.of(PropertyType.STRING, Multiplicity.from(1, 1), "'aaa'"));
        }});
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new HashMap<>() {{
            put("a", PropertyDefinition.of(PropertyType.STRING, Multiplicity.from(1, 1), "'aaa'"));
        }});
        this.sqlgGraph.tx().commit();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        a.addEdge("ab", b);
        this.sqlgGraph.tx().commit();
        List<String> values = this.sqlgGraph.traversal().V().hasLabel("A").<String>values("a").toList();
        Assert.assertEquals(1, values.size());
        Assert.assertEquals("aaa", values.get(0));
        values = this.sqlgGraph.traversal().V().hasLabel("B").<String>values("a").toList();
        Assert.assertEquals(1, values.size());
        Assert.assertEquals("aaa", values.get(0));
        values = this.sqlgGraph.traversal().E().hasLabel("ab").<String>values("a").toList();
        Assert.assertEquals(1, values.size());
        Assert.assertEquals("aaa", values.get(0));
    }
}
