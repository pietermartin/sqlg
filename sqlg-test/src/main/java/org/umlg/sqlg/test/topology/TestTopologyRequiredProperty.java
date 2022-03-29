package org.umlg.sqlg.test.topology;

import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;

public class TestTopologyRequiredProperty  extends BaseTest {

    @Test
    public void testRequiredProperty() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("A", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
        }});
    }
}
