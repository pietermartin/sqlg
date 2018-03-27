package org.umlg.sqlg.test.usersuppliedpk.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/03/20
 */
public class TestSimpleJoinGremlin extends BaseTest {

//    @Test
//    public void testSinglePath() {
//        VertexLabel person = this.sqlgGraph.getTopology().ensureVertexLabelExist(
//                "Person",
//                new HashMap<String, PropertyType>(){{
//                    put("name", PropertyType.STRING);
//                    put("surname", PropertyType.STRING);
//                }},
//                ListOrderedSet.listOrderedSet(Arrays.asList("name", "surname"))
//        );
//        VertexLabel address = this.sqlgGraph.getTopology().ensureVertexLabelExist(
//                "Address",
//                new HashMap<String, PropertyType>(){{
//                    put("street", PropertyType.STRING);
//                    put("suburb", PropertyType.STRING);
//                }},
//                ListOrderedSet.listOrderedSet(Arrays.asList("street", "suburb"))
//        );
//        EdgeLabel livesAt = person.ensureEdgeLabelExist(
//                "livesAt",
//                address,
//                new HashMap<String, PropertyType>() {{
//                    put("country", PropertyType.STRING);
//                }}
//        );
//        this.sqlgGraph.tx().commit();
//
//        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "John", "surname", "Smith");
//        Vertex address1 = this.sqlgGraph.addVertex(T.label, "Address", "street", "X", "suburb", "Y");
//        Edge livesAt1 = person1.addEdge("livesAt", address1, "country", "articania");
//        this.sqlgGraph.tx().commit();
//
//        List<Edge> livesAtEdges = this.sqlgGraph.traversal().V().hasLabel("Person").outE().toList();
//        Assert.assertEquals(1, livesAtEdges.size());
//        Assert.assertEquals(livesAt1, livesAtEdges.get(0));
//
//        List<Vertex> livesAts = this.sqlgGraph.traversal().V().hasLabel("Person").out().toList();
//        Assert.assertEquals(1, livesAts.size());
//        Assert.assertEquals(address1, livesAts.get(0));
//        Assert.assertEquals("X", livesAts.get(0).value("street"));
//        Assert.assertEquals("Y", livesAts.get(0).value("suburb"));
//
//        List<Map<String, Object>> result = this.sqlgGraph.traversal()
//                .V().hasLabel("Person").as("a")
//                .outE().as("b")
//                .otherV().as("c")
//                .select("a", "b", "c").toList();
//        Assert.assertEquals(1, result.size());
//        Assert.assertEquals(3, result.get(0).size());
//        Assert.assertEquals(person1, result.get(0).get("a"));
//        Assert.assertEquals(livesAt1, result.get(0).get("b"));
//        Assert.assertEquals(address1, result.get(0).get("c"));
//    }

    @Test
    public void testDuplicatePath() {
        this.sqlgGraph.tx().commit();
        VertexLabel person = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new HashMap<String, PropertyType>(){{
                    put("name", PropertyType.STRING);
                    put("surname", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name", "surname"))
        );
        EdgeLabel livesAt = person.ensureEdgeLabelExist(
                "loves",
                person,
                new HashMap<String, PropertyType>() {{
                    put("country", PropertyType.STRING);
                }}
        );
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").out().toList();
    }

}
