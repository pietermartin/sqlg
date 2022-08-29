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

public class TestRequiredProperty extends BaseTest {

    @Test
    public void testRequiredPropertyOnVertexLabel() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("a", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
                }}
        );
        this.sqlgGraph.tx().commit();
        boolean failure = false;
        try {
            this.sqlgGraph.addVertex(T.label, "A");
        } catch (Exception e) {
            failure = true;
            this.sqlgGraph.tx().rollback();
        }
        Assert.assertTrue(failure);
        failure = false;
        this.sqlgGraph.tx().normalBatchModeOn();
        try {
            this.sqlgGraph.addVertex(T.label, "A");
            this.sqlgGraph.tx().commit();
        } catch (Exception e) {
            failure = true;
            this.sqlgGraph.tx().rollback();
        }
        Assert.assertTrue(failure);
    }

    @Test
    public void testRequiredPropertyOnEdgeLabel() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("a", new PropertyDefinition(PropertyType.STRING));
                }}
        );
        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("a", new PropertyDefinition(PropertyType.STRING));
                }}
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new HashMap<>() {{
            put("a", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
        }});
        this.sqlgGraph.tx().commit();

        boolean failure = false;
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        try {
            a.addEdge("ab", b);
        } catch (Exception e ) {
            failure = true;
            this.sqlgGraph.tx().rollback();
        }
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(failure);
        failure = false;

        this.sqlgGraph.tx().normalBatchModeOn();
        a = this.sqlgGraph.addVertex(T.label, "A");
        b = this.sqlgGraph.addVertex(T.label, "B");
        try {
            a.addEdge("ab", b);
            this.sqlgGraph.tx().commit();
        } catch (Exception e ) {
            failure = true;
            this.sqlgGraph.tx().rollback();
        }
        Assert.assertTrue(failure);
    }
}
