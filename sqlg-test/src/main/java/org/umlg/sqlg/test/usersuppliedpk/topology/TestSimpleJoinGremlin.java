package org.umlg.sqlg.test.usersuppliedpk.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeLabel;
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

    @Test
    public void testSinglePath() {
        VertexLabel person = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new HashMap<String, PropertyType>(){{
                    put("name", PropertyType.varChar(100));
                    put("surname", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name", "surname"))
        );
        VertexLabel address = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Address",
                new HashMap<String, PropertyType>(){{
                    put("street", PropertyType.varChar(100));
                    put("suburb", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("street", "suburb"))
        );
        @SuppressWarnings("unused")
        EdgeLabel livesAt = person.ensureEdgeLabelExist(
                "livesAt",
                address,
                new HashMap<String, PropertyType>() {{
                    put("country", PropertyType.STRING);
                }}
        );
        this.sqlgGraph.tx().commit();

        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "John", "surname", "Smith");
        Vertex address1 = this.sqlgGraph.addVertex(T.label, "Address", "street", "X", "suburb", "Y");
        Edge livesAt1 = person1.addEdge("livesAt", address1, "country", "moon");
        this.sqlgGraph.tx().commit();

        List<Edge> livesAtEdges = this.sqlgGraph.traversal().V().hasLabel("Person").outE().toList();
        Assert.assertEquals(1, livesAtEdges.size());
        Assert.assertEquals(livesAt1, livesAtEdges.get(0));

        List<Vertex> livesAts = this.sqlgGraph.traversal().V().hasLabel("Person").out().toList();
        Assert.assertEquals(1, livesAts.size());
        Assert.assertEquals(address1, livesAts.get(0));
        Assert.assertEquals("X", livesAts.get(0).value("street"));
        Assert.assertEquals("Y", livesAts.get(0).value("suburb"));

        List<Map<String, Object>> result = this.sqlgGraph.traversal()
                .V().hasLabel("Person").as("a")
                .outE().as("b")
                .otherV().as("c")
                .select("a", "b", "c").toList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(3, result.get(0).size());
        Assert.assertEquals(person1, result.get(0).get("a"));
        Assert.assertEquals(livesAt1, result.get(0).get("b"));
        Assert.assertEquals(address1, result.get(0).get("c"));

        livesAtEdges = this.sqlgGraph.traversal().V().hasLabel("Address").inE().toList();
        Assert.assertEquals(1, livesAtEdges.size());
        Assert.assertEquals(livesAt1, livesAtEdges.get(0));

        List<Vertex> persons = this.sqlgGraph.traversal().V().hasLabel("Address").in().toList();
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(person1, persons.get(0));
        Assert.assertEquals("John", persons.get(0).value("name"));
        Assert.assertEquals("Smith", persons.get(0).value("surname"));

        result = this.sqlgGraph.traversal()
                .V().hasLabel("Address").as("a")
                .inE().as("b")
                .otherV().as("c")
                .select("a", "b", "c").toList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(3, result.get(0).size());
        Assert.assertEquals(address1, result.get(0).get("a"));
        Assert.assertEquals(livesAt1, result.get(0).get("b"));
        Assert.assertEquals(person1, result.get(0).get("c"));
    }

    @Test
    public void testDuplicatePath() {
        VertexLabel person = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new HashMap<String, PropertyType>(){{
                    put("name", PropertyType.varChar(100));
                    put("surname", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name", "surname"))
        );
        @SuppressWarnings("unused")
        EdgeLabel livesAt = person.ensureEdgeLabelExist(
                "loves",
                person,
                new HashMap<String, PropertyType>() {{
                    put("country", PropertyType.STRING);
                }}
        );
        this.sqlgGraph.tx().commit();

        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "John", "surname", "Smith");
        Vertex person2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "Suzi", "surname", "Lovenot");
        person1.addEdge("loves", person2);
        person2.addEdge("loves", person1);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").out().toList();
        Assert.assertEquals(2, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").in().toList();
        Assert.assertEquals(2, vertices.size());

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").outE().inV().toList();
        Assert.assertEquals(2, vertices.size());

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").outE().outV().toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testDuplicatePath2() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new HashMap<String, PropertyType>(){{
                    put("name", PropertyType.varChar(100));
                    put("name2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name", "name2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new HashMap<String, PropertyType>(){{
                    put("name", PropertyType.varChar(100));
                    put("name2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name", "name2"))
        );
        @SuppressWarnings("unused")
        EdgeLabel livesAt = aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "name2", "a11");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1", "name2", "b11");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "name2", "a22");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2", "name2", "b22");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").out().in().toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(a1));
        Assert.assertTrue(vertices.contains(a2));

        vertices = this.sqlgGraph.traversal().V().hasLabel("B").in().out().toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b1));
        Assert.assertTrue(vertices.contains(b2));
    }

}
