package org.umlg.sqlg.test;

import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.EdgeRole;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;

import java.util.HashMap;

public class TestTinkerPopSchemaProposal extends BaseTest {

    @Test
    public void test() {

        Schema schema = this.sqlgGraph.getTopology().getPublicSchema();

        VertexLabel aVertexLabel = schema.ensureVertexLabelExist("A", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
        }});
        VertexLabel bVertexLabel = schema.ensureVertexLabelExist("B", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
        }});
        VertexLabel cVertexLabel = schema.ensureVertexLabelExist("C", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
        }});

        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("loves", bVertexLabel);
        edgeLabel = aVertexLabel.ensureEdgeLabelExist("loves", cVertexLabel);

        this.sqlgGraph.tx().commit();

        schema = this.sqlgGraph.getTopology().getPublicSchema();
        aVertexLabel = schema.getVertexLabel("A").orElseThrow();
        bVertexLabel = schema.getVertexLabel("B").orElseThrow();
        EdgeRole outEdgeRole = aVertexLabel.getOutEdgeRoles().get("public.loves");
        System.out.println(outEdgeRole.getDirection().toString());
        EdgeRole inEdgeRole = bVertexLabel.getInEdgeRoles().get("public.loves");
        System.out.println(inEdgeRole.getDirection().toString());


//        edgeRole.remove();

//        edgeLabel = schema.getEdgeLabel("loves").orElseThrow();
//        EdgeRole edgeRole = edgeLabel.getOutEdgeRoles(aVertexLabel);
//        Assert.assertNotNull(edgeRole);

    }

}
