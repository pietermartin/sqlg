package org.umlg.sqlg.test.process.dropstep;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2019/08/26
 */
public class TestPartitionedDrop extends BaseTest {

    @Before
    public void before() throws Exception {
        super.before();
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsPartitioning());
    }

    private enum VENDOR_TECHNOLOGY {
        HGSM,
        HUMTS,
        HLTE,
        HCOMPT
    }

    @SuppressWarnings({"unused", "UnusedAssignment"})
    @Test
    public void testPartitionEdgeOnUserDefinedForeignKey() {
        LinkedHashMap<String, PropertyType> attributeMap = new LinkedHashMap<>();
        attributeMap.put("name", PropertyType.STRING);
        attributeMap.put("cmUid", PropertyType.STRING);
        attributeMap.put("vendorTechnology", PropertyType.STRING);

        VertexLabel realWorkspaceElementVertexLabel = sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "RealWorkspaceElement",
                attributeMap,
                ListOrderedSet.listOrderedSet(List.of("cmUid", "vendorTechnology")),
                PartitionType.LIST,
                "\"vendorTechnology\""
        );
        for (VENDOR_TECHNOLOGY vendorTechnology : VENDOR_TECHNOLOGY.values()) {
            Partition partition = realWorkspaceElementVertexLabel.ensureListPartitionExists(
                    vendorTechnology.name(),
                    "'" + vendorTechnology.name() + "'"
            );
        }
        PropertyColumn propertyColumn = realWorkspaceElementVertexLabel.getProperty("cmUid").orElseThrow(IllegalStateException::new);
        realWorkspaceElementVertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(propertyColumn));

        VertexLabel virtualGroupVertexLabel = sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "VirtualGroup",
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.STRING);
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );

        EdgeLabel edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedEdgeLabelExistOnInOrOutVertexLabel(
                "virtualGroup_RealWorkspaceElement",
                virtualGroupVertexLabel,
                realWorkspaceElementVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid")),
                PartitionType.LIST,
                virtualGroupVertexLabel
        );
        this.sqlgGraph.tx().commit();

        Vertex northern = this.sqlgGraph.addVertex(T.label, "VirtualGroup", "uid", UUID.randomUUID().toString(), "name", "Northern");
        Partition partition = edgeLabel.ensureListPartitionExists(
                "Northern",
                "'" + ((RecordId) northern.id()).getID().getIdentifiers().get(0).toString() + "'"
        );
        Vertex western = this.sqlgGraph.addVertex(T.label, "VirtualGroup", "uid", UUID.randomUUID().toString(), "name", "Western");
        partition = edgeLabel.ensureListPartitionExists(
                "Western",
                "'" + ((RecordId) western.id()).getID().getIdentifiers().get(0).toString() + "'"
        );
        this.sqlgGraph.tx().commit();

        List<String> hgsmCmUid = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Vertex hgsm = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "a" + i, "name", "hgsm" + i, "vendorTechnology", VENDOR_TECHNOLOGY.HGSM.name());
            hgsmCmUid.add("a" + i);
            Vertex humts = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "b" + i, "name", "humts" + i, "vendorTechnology", VENDOR_TECHNOLOGY.HUMTS.name());
            Vertex hlte = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "c" + i, "name", "hlte" + i, "vendorTechnology", VENDOR_TECHNOLOGY.HLTE.name());
            Vertex hcompt = this.sqlgGraph.addVertex(T.label, "RealWorkspaceElement", "cmUid", "d" + i, "name", "hcompt" + i, "vendorTechnology", VENDOR_TECHNOLOGY.HCOMPT.name());

            if (i % 2 == 0) {
                Edge e = northern.addEdge("virtualGroup_RealWorkspaceElement", hgsm, "uid", UUID.randomUUID().toString());
                e = northern.addEdge("virtualGroup_RealWorkspaceElement", humts, "uid", UUID.randomUUID().toString());
                e = northern.addEdge("virtualGroup_RealWorkspaceElement", hlte, "uid", UUID.randomUUID().toString());
                e = northern.addEdge("virtualGroup_RealWorkspaceElement", hcompt, "uid", UUID.randomUUID().toString());
            } else {
                Edge e = western.addEdge("virtualGroup_RealWorkspaceElement", hgsm, "uid", UUID.randomUUID().toString());
                e = western.addEdge("virtualGroup_RealWorkspaceElement", humts, "uid", UUID.randomUUID().toString());
                e = western.addEdge("virtualGroup_RealWorkspaceElement", hlte, "uid", UUID.randomUUID().toString());
                e = western.addEdge("virtualGroup_RealWorkspaceElement", hcompt, "uid", UUID.randomUUID().toString());
            }
        }

        this.sqlgGraph.tx().commit();

        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", VENDOR_TECHNOLOGY.HGSM.name()).count().next(), 0);
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", VENDOR_TECHNOLOGY.HUMTS.name()).count().next(), 0);
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", VENDOR_TECHNOLOGY.HLTE.name()).count().next(), 0);
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("RealWorkspaceElement").has("vendorTechnology", VENDOR_TECHNOLOGY.HCOMPT.name()).count().next(), 0);

        List<Object> ids =  this.sqlgGraph.traversal()
                .V(northern)
                .outE("virtualGroup_RealWorkspaceElement")
                .as("e")
                .otherV()
                .has("vendorTechnology", VENDOR_TECHNOLOGY.HGSM.name())
                .select("e")
                .by(T.id)
                .toList();
        Assert.assertEquals(5, ids.size());

        this.sqlgGraph.traversal().V()
                .hasLabel("RealWorkspaceElement")
                .has("vendorTechnology", VENDOR_TECHNOLOGY.HGSM.name())
                .drop()
                .iterate();

        this.sqlgGraph.tx().commit();
        ids =  this.sqlgGraph.traversal()
                .V(northern)
                .outE("virtualGroup_RealWorkspaceElement")
                .as("e")
                .otherV()
                .has("vendorTechnology", VENDOR_TECHNOLOGY.HGSM.name())
                .select("e")
                .by(T.id)
                .toList();
        Assert.assertEquals(0, ids.size());
        List<Vertex> realWorkspaceVertices =  this.sqlgGraph.traversal()
                .V(northern)
                .out("virtualGroup_RealWorkspaceElement")
                .has("vendorTechnology", VENDOR_TECHNOLOGY.HCOMPT.name())
                .toList();
        Assert.assertEquals(5, realWorkspaceVertices.size());

        this.sqlgGraph.traversal().V()
                .hasLabel("RealWorkspaceElement")
                .has("vendorTechnology", VENDOR_TECHNOLOGY.HCOMPT.name())
                .drop()
                .iterate();
        realWorkspaceVertices =  this.sqlgGraph.traversal()
                .V(northern)
                .out("virtualGroup_RealWorkspaceElement")
                .has("vendorTechnology", VENDOR_TECHNOLOGY.HGSM.name())
                .toList();
        Assert.assertEquals(0, realWorkspaceVertices.size());
    }
}
