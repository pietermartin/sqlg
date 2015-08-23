package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;

/**
 * Created by pieter on 2015/08/22.
 */
public class TestAggregate extends BaseTest {

    @Test
    public void testAggregate() {
        Vertex group = this.sqlgGraph.addVertex(T.label, "Group", "name", "MTN");
        Vertex network = this.sqlgGraph.addVertex(T.label, "Network", "name", "SouthAfrica");
        Vertex networkSoftwareVersion = this.sqlgGraph.addVertex(T.label, "NetworkSoftwareVersion", "name", "SouthAfricaHuawei");
        group.addEdge("groupNetwork", network);
        network.addEdge("networkNetworkSoftwareVersion", networkSoftwareVersion);
        Vertex networkNodeGroupBsc = this.sqlgGraph.addVertex(T.label, "NetworkNodeGroup", "name", "BSC");
        Vertex networkNodeGroupRnc = this.sqlgGraph.addVertex(T.label, "NetworkNodeGroup", "name", "RNC");
        networkSoftwareVersion.addEdge("networkSoftwareVersionNetworkNodeGroup", networkNodeGroupBsc);
        networkSoftwareVersion.addEdge("networkSoftwareVersionNetworkNodeGroup", networkNodeGroupRnc);
        Vertex bsc1 = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "BSC1");
        Vertex bsc2 = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "BSC2");
        Vertex bsc3 = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "BSC3");
        Vertex bsc4 = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "BSC4");
        Vertex rnc1 = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "RNC1");
        Vertex rnc2 = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "RNC2");
        Vertex rnc3 = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "RNC3");
        Vertex rnc4 = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "RNC4");
//        networkNodeGroupBsc.addEdge("networkNodeGroupNetworkNode", bsc1);
//        networkNodeGroupBsc.addEdge("networkNodeGroupNetworkNode", bsc2);
//        networkNodeGroupBsc.addEdge("networkNodeGroupNetworkNode", bsc3);
//        networkNodeGroupBsc.addEdge("networkNodeGroupNetworkNode", bsc4);
        networkNodeGroupRnc.addEdge("networkNodeGroupNetworkNode", rnc1);
        networkNodeGroupRnc.addEdge("networkNodeGroupNetworkNode", rnc2);
        networkNodeGroupRnc.addEdge("networkNodeGroupNetworkNode", rnc3);
        networkNodeGroupRnc.addEdge("networkNodeGroupNetworkNode", rnc4);
        this.sqlgGraph.tx().commit();

        List<Map<String, Vertex>> result = this.sqlgGraph.traversal().V()
                .hasLabel("Group").as("g")
                .out("groupNetwork").as("network")
                .out("networkNetworkSoftwareVersion").as("nsv")
                .out("networkSoftwareVersionNetworkNodeGroup").aggregate("nng")
                .out("networkNodeGroupNetworkNode").as("nn")
                .<Vertex>select("g", "network", "nsv", "nng", "nn")
                .toList();

        System.out.println(result);
    }
}
