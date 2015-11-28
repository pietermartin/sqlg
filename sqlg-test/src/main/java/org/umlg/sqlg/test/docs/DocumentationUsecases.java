package org.umlg.sqlg.test.docs;

import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.predicate.Text;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Created by pieter on 2015/11/27.
 */
public class DocumentationUsecases extends BaseTest {

//    @Test
//    public void schemaDoc() {
//        Vertex john = this.sqlgGraph.addVertex(T.label, "manager", "name", "john");
//        Vertex palace1 = this.sqlgGraph.addVertex(T.label, "continent.house", "name", "palace1");
//        Vertex corrola = this.sqlgGraph.addVertex(T.label, "fleet.car", "model", "corrola");
//        palace1.addEdge("managedBy", john);
//        corrola.addEdge("owner", john);
//        this.sqlgGraph.tx().commit();
//    }
//
//    @Test
//    public void testIndexOnVertex() throws SQLException {
//        this.sqlgGraph.createVertexLabeledIndex("Person", "name", "dummy");
//        this.sqlgGraph.tx().commit();
//        for (int i = 0; i < 5000; i++) {
//            this.sqlgGraph.addVertex(T.label, "Person", "name", "john" + i);
//        }
//        this.sqlgGraph.tx().commit();
//        assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "Person").has("name", "john50").count().next(), 0);
//
//        //Check if the index is being used
//        Connection conn = this.sqlgGraph.tx().getConnection();
//        Statement statement = conn.createStatement();
//        ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"public\".\"V_Person\" a WHERE a.\"name\" = 'john50'");
//        assertTrue(rs.next());
//        String result = rs.getString(1);
//        System.out.println(result);
//        assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
//        statement.close();
//        this.sqlgGraph.tx().rollback();
//    }
//
//    @Test
//    public void testIndexOnVertex22() throws SQLException {
//        this.sqlgGraph.tx().commit();
//        for (int i = 0; i < 5000; i++) {
//            this.sqlgGraph.addVertex(T.label, "Person", "name", "john" + i);
//        }
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.createVertexLabeledIndex("Person", "name", "dummy");
//        this.sqlgGraph.tx().commit();
//        assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "Person").has("name", "john50").count().next(), 0);
//
//        //Check if the index is being used
//        Connection conn = this.sqlgGraph.tx().getConnection();
//        Statement statement = conn.createStatement();
//        ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"public\".\"V_Person\" a WHERE a.\"name\" = 'john50'");
//        assertTrue(rs.next());
//        String result = rs.getString(1);
//        System.out.println(result);
//        assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
//        statement.close();
//        this.sqlgGraph.tx().rollback();
//    }
//
//    @Test
//    public void showHighLatency() {
//        Vertex easternUnion = this.sqlgGraph.addVertex(T.label, "Organization", "name", "EasternUnion");
//        Vertex legal = this.sqlgGraph.addVertex(T.label, "Division", "name", "Legal");
//        Vertex dispatch = this.sqlgGraph.addVertex(T.label, "Division", "name", "Dispatch");
//        Vertex newYork = this.sqlgGraph.addVertex(T.label, "Office", "name", "NewYork");
//        Vertex singapore = this.sqlgGraph.addVertex(T.label, "Office", "name", "Singapore");
//        easternUnion.addEdge("organization_division", legal);
//        easternUnion.addEdge("organization_division", dispatch);
//        legal.addEdge("division_office", newYork);
//        dispatch.addEdge("division_office", singapore);
//        this.sqlgGraph.tx().commit();
//        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasLabel("Organization").out().out();
//        System.out.println(traversal);
//        traversal.hasNext();
//        System.out.println(traversal);
//        List<Vertex> offices = traversal.toList();
//        assertEquals(2, offices.size());
//    }
//
//    @Test
//    public void showComparePredicate() {
//        Vertex easternUnion = this.sqlgGraph.addVertex(T.label, "Organization", "name", "EasternUnion");
//        Vertex legal = this.sqlgGraph.addVertex(T.label, "Division", "name", "Legal");
//        Vertex dispatch = this.sqlgGraph.addVertex(T.label, "Division", "name", "Dispatch");
//        Vertex newYork = this.sqlgGraph.addVertex(T.label, "Office", "name", "NewYork");
//        Vertex singapore = this.sqlgGraph.addVertex(T.label, "Office", "name", "Singapore");
//        easternUnion.addEdge("organization_division", legal);
//        easternUnion.addEdge("organization_division", dispatch);
//        legal.addEdge("division_office", newYork);
//        dispatch.addEdge("division_office", singapore);
//        this.sqlgGraph.tx().commit();
//
//        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasLabel("Organization").out().out().has("name", P.eq("Singapore"));
//        System.out.println(traversal);
//        traversal.hasNext();
//        System.out.println(traversal);
//        List<Vertex> offices = traversal.toList();
//        assertEquals(1, offices.size());
//        assertEquals(singapore, offices.get(0));
//    }
//
//    @Test
//    public void showContainsPredicate() {
//        List<Integer> numbers = new ArrayList<>(10000);
//        for (int i = 0; i < 10000; i++) {
//            this.sqlgGraph.addVertex(T.label, "Person", "number", i);
//            numbers.add(i);
//        }
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> persons = this.sqlgGraph.traversal().V()
//                .hasLabel("Person")
//                .has("number", P.within(numbers))
//                .toList();
//        assertEquals(10000, persons.size());
//    }
//
//    @Test
//    public void showTextPredicate() {
//        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "John XXX Doe");
//        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "Peter YYY Snow");
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> persons = this.sqlgGraph.traversal().V()
//                .hasLabel("Person")
//                .has("name", Text.contains("XXX")).toList();
//
//        assertEquals(1, persons.size());
//        assertEquals(john, persons.get(0));
//    }
//
//    @Test
//    public void showSearchOnLocalDateTime() {
//        LocalDateTime born1 = LocalDateTime.of(1990, 1, 1, 1, 1, 1);
//        LocalDateTime born2 = LocalDateTime.of(1990, 1, 1, 1, 1, 2);
//        LocalDateTime born3 = LocalDateTime.of(1990, 1, 1, 1, 1, 3);
//        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "John", "born", born1);
//        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "Peter", "born", born2);
//        Vertex paul = this.sqlgGraph.addVertex(T.label, "Person", "name", "Paul", "born", born3);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> persons = this.sqlgGraph.traversal().V().hasLabel("Person")
//                .has("born", P.eq(born1))
//                .toList();
//        assertEquals(1, persons.size());
//        assertEquals(john, persons.get(0));
//
//        persons = this.sqlgGraph.traversal().V().hasLabel("Person")
//                .has("born", P.between(LocalDateTime.of(1990, 1, 1, 1, 1, 1), LocalDateTime.of(1990, 1, 1, 1, 1, 3)))
//                .toList();
//        //P.between is inclusive to exclusive
//        assertEquals(2, persons.size());
//        assertTrue(persons.contains(john));
//        assertTrue(persons.contains(peter));
//    }
//
//    @Test
//    public void testOrderBy() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "surname", "a");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "surname", "b");
//        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "surname", "c");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "A", "name", "b", "surname", "a");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "A", "name", "b", "surname", "b");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "A", "name", "b", "surname", "c");
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> result = this.sqlgGraph.traversal().V().hasLabel("A")
//                .order().by("name", Order.incr).by("surname", Order.decr)
//                .toList();
//
//        assertEquals(6, result.size());
//        assertEquals(a3, result.get(0));
//        assertEquals(a2, result.get(1));
//        assertEquals(a1, result.get(2));
//        assertEquals(b3, result.get(3));
//        assertEquals(b2, result.get(4));
//        assertEquals(b1, result.get(5));
//    }

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
                .by(__.select("g").by("name"), Order.incr)
                .by(__.select("network").by("name"), Order.incr)
                .by(__.select("nsv").by("name"), Order.incr)
                .by(__.select("nng").by("name"), Order.incr)
                .by(__.select("nn").by("name"), Order.decr)
                .toList();

        for (Map<String, Vertex> stringVertexMap : result) {
            System.out.println(stringVertexMap.get("g").<String>value("name") + " " +
                    stringVertexMap.get("network").<String>value("name") + " " +
                    stringVertexMap.get("nsv").<String>value("name") + " " +
                    stringVertexMap.get("nng").<String>value("name") + " " +
                    stringVertexMap.get("nn").<String>value("name")
            );
        }

        assertEquals(8, result.size());
        Map<String,Vertex> row1 = result.get(0);
        assertEquals("BSC", row1.get("nng").value("name"));
        assertEquals("BSCD", row1.get("nn").value("name"));
        Map<String,Vertex> row2 = result.get(1);
        assertEquals("BSC", row2.get("nng").value("name"));
        assertEquals("BSCC", row2.get("nn").value("name"));
        Map<String,Vertex> row3 = result.get(2);
        assertEquals("BSC", row3.get("nng").value("name"));
        assertEquals("BSCB", row3.get("nn").value("name"));
        Map<String,Vertex> row4 = result.get(3);
        assertEquals("BSC", row4.get("nng").value("name"));
        assertEquals("BSCA", row4.get("nn").value("name"));
        Map<String,Vertex> row5 = result.get(4);
        assertEquals("RNC", row5.get("nng").value("name"));
        assertEquals("RNCD", row5.get("nn").value("name"));
        Map<String,Vertex> row6 = result.get(5);
        assertEquals("RNC", row6.get("nng").value("name"));
        assertEquals("RNCC", row6.get("nn").value("name"));
        Map<String,Vertex> row7 = result.get(6);
        assertEquals("RNC", row7.get("nng").value("name"));
        assertEquals("RNCB", row7.get("nn").value("name"));
        Map<String,Vertex> row8 = result.get(7);
        assertEquals("RNC", row8.get("nng").value("name"));
        assertEquals("RNCA", row8.get("nn").value("name"));
    }
}
