package org.umlg.sqlg.test.usersuppliedpk.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/03/20
 */
public class TestSimpleJoinGremlin extends BaseTest {

//    @Test
    public void testVertexStep() {
        VertexLabel person = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new HashMap<String, PropertyType>(){{
                    put("name", PropertyType.STRING);
                    put("surname", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name", "surname"))
        );
        VertexLabel address = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Address",
                new HashMap<String, PropertyType>(){{
                    put("street", PropertyType.STRING);
                    put("suburb", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("street", "suburb"))
        );
        person.ensureEdgeLabelExist(
                "livesAt",
                address
        );
        this.sqlgGraph.tx().commit();

        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "John", "surname", "Smith");
        Vertex address1 = this.sqlgGraph.addVertex(T.label, "Address", "street", "X", "suburb", "Y");
        person1.addEdge("livesAt", address1);
        this.sqlgGraph.tx().commit();

        List<Vertex> addresses = this.sqlgGraph.traversal().V().hasLabel("Person").out().toList();
        Assert.assertEquals(1, addresses.size());
        List<Vertex> persons = this.sqlgGraph.traversal().V().hasLabel("Address").in().toList();
        Assert.assertEquals(1, persons.size());
        List<Map<String, Vertex>>  personAddressMap = this.sqlgGraph.traversal()
                .V().hasLabel("Person").as("person")
                .out().as("address")
                .<Vertex>select("person", "address")
                .toList();
        Assert.assertEquals(1, personAddressMap.size());
        Assert.assertTrue(personAddressMap.get(0).containsKey("person"));
        Assert.assertTrue(personAddressMap.get(0).containsKey("address"));
        Assert.assertEquals(person1, personAddressMap.get(0).get("person"));
        Assert.assertEquals(address1, personAddressMap.get(0).get("address"));
    }

//    @Test
    public void testEdgeStep() {
        VertexLabel person = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new HashMap<String, PropertyType>(){{
                    put("name", PropertyType.STRING);
                    put("surname", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name", "surname"))
        );
        VertexLabel address = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Address",
                new HashMap<String, PropertyType>(){{
                    put("street", PropertyType.STRING);
                    put("suburb", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("street", "suburb"))
        );
        person.ensureEdgeLabelExist(
                "livesAt",
                address,
                new HashMap<String, PropertyType>() {{
                    put("country", PropertyType.STRING);
                }}
        );
        this.sqlgGraph.tx().commit();

        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "John", "surname", "Smith");
        Vertex address1 = this.sqlgGraph.addVertex(T.label, "Address", "street", "X", "suburb", "Y");
        person1.addEdge("livesAt", address1, "country", "articania");
        this.sqlgGraph.tx().commit();

        List<Edge>  livesAtEdges = this.sqlgGraph.traversal().V().hasLabel("Person").outE().toList();
        Assert.assertEquals(1, livesAtEdges.size());
    }

    @Test
    public void testDuplicatePath() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        a1.addEdge("aa", a2, "name", "ee");
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").out().toList();
    }

//    @Test
    public void testJoinOld() {
        VertexLabel person = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new HashMap<String, PropertyType>(){{
                    put("name", PropertyType.STRING);
                    put("surname", PropertyType.STRING);
                }}
        );
        VertexLabel address = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Address",
                new HashMap<String, PropertyType>(){{
                    put("street", PropertyType.STRING);
                    put("suburb", PropertyType.STRING);
                }}
        );
        person.ensureEdgeLabelExist(
                "livesAt",
                address,
                new HashMap<String, PropertyType>() {{
                    put("country", PropertyType.STRING);
                }}
        );
        this.sqlgGraph.tx().commit();

        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "John", "surname", "Smith");
        Vertex address1 = this.sqlgGraph.addVertex(T.label, "Address", "street", "X", "suburb", "Y");
        person1.addEdge("livesAt", address1, "country", "articania");
        this.sqlgGraph.tx().commit();

        List<Edge> livesAtEdges = this.sqlgGraph.traversal().V().hasLabel("Person").outE().toList();
        Assert.assertEquals(1, livesAtEdges.size());
    }
}
