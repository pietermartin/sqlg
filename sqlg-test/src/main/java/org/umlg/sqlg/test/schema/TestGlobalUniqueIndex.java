package org.umlg.sqlg.test.schema;

import org.junit.Test;
import org.umlg.sqlg.structure.PropertyColumn;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.test.BaseTest;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Date: 2016/12/03
 * Time: 9:08 PM
 */
public class TestGlobalUniqueIndex extends BaseTest {

    @Test
    public void testGlobalUniqueIndex() {
        Map<String, PropertyType> properties = new HashMap<>();
        properties.put("namec", PropertyType.STRING);
        properties.put("namea", PropertyType.STRING);
        properties.put("nameb", PropertyType.STRING);
        this.sqlgGraph.getTopology().ensureVertexLabelExist("A", properties);
        this.sqlgGraph.tx().commit();

        Collection<PropertyColumn> propertyColumns = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getProperties().values();
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<>(propertyColumns));
        this.sqlgGraph.tx().commit();
    }
}
