package org.umlg.sqlg.test.tree;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/06/30
 * Time: 7:29 PM
 */
public class TestColumnNamePropertyNameMapScope extends BaseTest {

    /**
     * This tests a bug with the columnName/propertyName maps having the wrong scope.
     */
    @Test
    public void testColumnNamePropertyNameMap() {
        Vertex group = this.sqlgGraph.addVertex(T.label, "Group", "name", "group1", "className", "this.that.Group", "uid", UUID.randomUUID().toString());
        Vertex network = this.sqlgGraph.addVertex(T.label, "Network", "name", "network1", "className", "this.that.Network", "uid", UUID.randomUUID().toString());
        group.addEdge("group_network", network);
        for (int i = 0; i < 10; i++) {
            Vertex nsv = this.sqlgGraph.addVertex(T.label, "NetworkSoftwareVersion",
                    "name", "R15_HUAWEI_GSM" + i,
                    "className", "this.that.NetworkSoftwareVersion",
                    "firstLoad", false,
                    "softwareVersion", "R15_HUAWEI_GSM",
                    "uid", UUID.randomUUID().toString());
            network.addEdge("network_networkSoftwareVersion", nsv);
        }
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.traversal().V(network.id()).out("network_networkNodeGroup").<Vertex>forEachRemaining(Element::remove);

        this.sqlgGraph.tx().commit();

        List<Vertex> vertexList = sqlgGraph.traversal().V()
                .hasLabel("Group")
                .emit().repeat(__.out(
                        "group_network",
                        "network_networkSoftwareVersion"
                ))
                .times(5)
                .toList();
        System.out.println(vertexList);
        assertEquals("this.that.Network", vertexList.get(1).value("className"));
    }
}
