package org.umlg.sqlg.test.topology.propertydefinition;

import org.junit.Test;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.test.BaseTest;

import java.util.LinkedHashMap;

public class TestPropertyDefinitionMultiplicity extends BaseTest  {

    @Test(expected = IllegalArgumentException.class)
    public void testLowerMultiplicityBiggerThanUpper() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A",
                new LinkedHashMap<>() {{
                    put("a", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(111, 1)));
                }});
        this.sqlgGraph.tx().commit();
    }

}
