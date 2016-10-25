package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;

/**
 * Created by pieter on 2015/08/22.
 */
public class TestGraphStepOrderBy extends BaseTest {

    @Test
    public void testOrderBy() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "surname", "a");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "surname", "b");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "surname", "c");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "A", "name", "b", "surname", "a");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "A", "name", "b", "surname", "b");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "A", "name", "b", "surname", "c");
        this.sqlgGraph.tx().commit();

        List<Vertex> result = this.sqlgGraph.traversal().V().hasLabel("A")
                .order()
                .by("name", Order.incr).by("surname", Order.decr)
                .toList();

        Assert.assertEquals(6, result.size());
        Assert.assertEquals(a3, result.get(0));
        Assert.assertEquals(a2, result.get(1));
        Assert.assertEquals(a1, result.get(2));
        Assert.assertEquals(b3, result.get(3));
        Assert.assertEquals(b2, result.get(4));
        Assert.assertEquals(b1, result.get(5));
    }

    @Test
    public void testOrderBy2() {
        Vertex group = this.sqlgGraph.addVertex(T.label, "Group", "name", "MTN");
        Vertex network = this.sqlgGraph.addVertex(T.label, "Network", "name", "SouthAfrica");
        Vertex networkSoftwareVersion = this.sqlgGraph.addVertex(T.label, "NetworkSoftwareVersion", "name", "SouthAfricaHuawei");
        group.addEdge("groupNetwork", network);
        network.addEdge("networkNetworkSoftwareVersion", networkSoftwareVersion);
        Vertex networkNodeGroupBsc = this.sqlgGraph.addVertex(T.label, "NetworkNodeGroup", "name", "BSC");
        Vertex networkNodeGroupRnc = this.sqlgGraph.addVertex(T.label, "NetworkNodeGroup", "name", "RNC");
        networkSoftwareVersion.addEdge("networkSoftwareVersionNetworkNodeGroup", networkNodeGroupBsc);
        networkSoftwareVersion.addEdge("networkSoftwareVersionNetworkNodeGroup", networkNodeGroupRnc);
        Vertex bsc1 = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "BSCA");
        Vertex bsc2 = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "BSCB");
        Vertex bsc3 = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "BSCC");
        Vertex bsc4 = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "BSCD");
        Vertex rnc1 = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "RNCA");
        Vertex rnc2 = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "RNCB");
        Vertex rnc3 = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "RNCC");
        Vertex rnc4 = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "RNCD");
        networkNodeGroupBsc.addEdge("networkNodeGroupNetworkNode", bsc1);
        networkNodeGroupBsc.addEdge("networkNodeGroupNetworkNode", bsc2);
        networkNodeGroupBsc.addEdge("networkNodeGroupNetworkNode", bsc3);
        networkNodeGroupBsc.addEdge("networkNodeGroupNetworkNode", bsc4);
        networkNodeGroupRnc.addEdge("networkNodeGroupNetworkNode", rnc1);
        networkNodeGroupRnc.addEdge("networkNodeGroupNetworkNode", rnc2);
        networkNodeGroupRnc.addEdge("networkNodeGroupNetworkNode", rnc3);
        networkNodeGroupRnc.addEdge("networkNodeGroupNetworkNode", rnc4);
        this.sqlgGraph.tx().commit();

        List<Map<String, Vertex>> result = this.sqlgGraph.traversal().V()
                .hasLabel("Group").as("g")
                .out("groupNetwork").as("network")
                .out("networkNetworkSoftwareVersion").as("nsv")
                .out("networkSoftwareVersionNetworkNodeGroup").as("nng")
                .out("networkNodeGroupNetworkNode").as("nn")
                .<Vertex>select("g", "network", "nsv", "nng", "nn")
                .order()
                .by(select("g").by("name"), Order.incr)
                .by(select("network").by("name"), Order.incr)
                .by(select("nsv").by("name"), Order.incr)
                .by(select("nng").by("name"), Order.incr)
                .by(select("nn").by("name"), Order.decr)
                .toList();

        for (Map<String, Vertex> stringVertexMap : result) {
            System.out.println(stringVertexMap.get("g").<String>value("name") + " " +
                    stringVertexMap.get("network").<String>value("name") + " " +
                            stringVertexMap.get("nsv").<String>value("name") + " " +
                            stringVertexMap.get("nng").<String>value("name") + " " +
                            stringVertexMap.get("nn").<String>value("name")
            );
        }

        Assert.assertEquals(8, result.size());
        Map<String,Vertex> row1 = result.get(0);
        Assert.assertEquals("BSC", row1.get("nng").value("name"));
        Assert.assertEquals("BSCD", row1.get("nn").value("name"));
        Map<String,Vertex> row2 = result.get(1);
        Assert.assertEquals("BSC", row2.get("nng").value("name"));
        Assert.assertEquals("BSCC", row2.get("nn").value("name"));
        Map<String,Vertex> row3 = result.get(2);
        Assert.assertEquals("BSC", row3.get("nng").value("name"));
        Assert.assertEquals("BSCB", row3.get("nn").value("name"));
        Map<String,Vertex> row4 = result.get(3);
        Assert.assertEquals("BSC", row4.get("nng").value("name"));
        Assert.assertEquals("BSCA", row4.get("nn").value("name"));
        Map<String,Vertex> row5 = result.get(4);
        Assert.assertEquals("RNC", row5.get("nng").value("name"));
        Assert.assertEquals("RNCD", row5.get("nn").value("name"));
        Map<String,Vertex> row6 = result.get(5);
        Assert.assertEquals("RNC", row6.get("nng").value("name"));
        Assert.assertEquals("RNCC", row6.get("nn").value("name"));
        Map<String,Vertex> row7 = result.get(6);
        Assert.assertEquals("RNC", row7.get("nng").value("name"));
        Assert.assertEquals("RNCB", row7.get("nn").value("name"));
        Map<String,Vertex> row8 = result.get(7);
        Assert.assertEquals("RNC", row8.get("nng").value("name"));
        Assert.assertEquals("RNCA", row8.get("nn").value("name"));
    }

    @Test
    public void testOrderby3() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "aa");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "ab");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "ba");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "bb");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "bc");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "bd");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a2.addEdge("ab", b3);
        a2.addEdge("ab", b4);
        this.sqlgGraph.tx().commit();

        List<Map<String, Vertex>> result = this.sqlgGraph.traversal().V()
                .hasLabel("A").as("a")
                .out("ab").as("b")
                .<Vertex>select("a", "b")
                .order()
                .by(select("a").by("name"), Order.incr)
                .by(select("b").by("name"), Order.decr)
                .toList();

        Assert.assertEquals(4, result.size());
        Map<String, Vertex> map1 = result.get(0);
        Map<String, Vertex> map2 = result.get(1);
        Map<String, Vertex> map3 = result.get(2);
        Map<String, Vertex> map4 = result.get(3);
        Assert.assertEquals("aa", map1.get("a").value("name"));
        Assert.assertEquals("bb", map1.get("b").value("name"));
        Assert.assertEquals("aa", map2.get("a").value("name"));
        Assert.assertEquals("ba", map2.get("b").value("name"));
        Assert.assertEquals("ab", map3.get("a").value("name"));
        Assert.assertEquals("bd", map3.get("b").value("name"));
        Assert.assertEquals("ab", map4.get("a").value("name"));
        Assert.assertEquals("bc", map4.get("b").value("name"));
    }

}
