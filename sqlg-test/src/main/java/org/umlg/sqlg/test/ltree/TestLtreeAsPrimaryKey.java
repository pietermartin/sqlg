package org.umlg.sqlg.test.ltree;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.PartitionType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class TestLtreeAsPrimaryKey extends BaseTest {

    @Test
    public void testLtreeAsPrimaryKey() {
        Assume.assumeTrue(isPostgres());
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Tree",
                        new HashMap<>() {{
                            put("vt", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
                            put("path", PropertyDefinition.of(PropertyType.LTREE, Multiplicity.of(1, 1)));
                        }},
                        ListOrderedSet.listOrderedSet(List.of("vt", "path"))
                );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Tree", "vt", "ERICSSON_GSM", "path", "path1");
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Tree").toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testLtreePartitionKey() {
        VertexLabel yangMetaModelTreeVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensurePartitionedVertexLabelExist(
                        "YangMetaModelTree",
                        new LinkedHashMap<>() {{
                            put("vendorTechnology", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
                            put("path", PropertyDefinition.of(PropertyType.LTREE, Multiplicity.of(1, 1)));
                            put("description", PropertyDefinition.of(PropertyType.STRING));
                        }},

                        ListOrderedSet.listOrderedSet(List.of("vendorTechnology", "path")),
                        PartitionType.LIST,
                        "\"vendorTechnology\""
                );
        for (String vendorTechnology : List.of("ERICSSON_GSM", "HUAWEI_GSM")) {
            yangMetaModelTreeVertexLabel.ensureListPartitionExists(vendorTechnology, "'" + vendorTechnology + "'");
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(
                T.label, "YangMetaModelTree",
                "vendorTechnology", "ERICSSON_GSM",
                "path", "path1",
                "description", "asdasd"
        );
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("YangMetaModelTree").toList();
        Assert.assertEquals(1, vertices.size());
    }
}
