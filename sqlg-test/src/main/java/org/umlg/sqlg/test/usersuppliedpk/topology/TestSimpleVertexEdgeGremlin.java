package org.umlg.sqlg.test.usersuppliedpk.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/03/18
 */
public class TestSimpleVertexEdgeGremlin extends BaseTest {

    @Test
    public void testSimpleVertexInsertAndUpdateAndQuery() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(100)));
                    put("surname", PropertyDefinition.of(PropertyType.varChar(100)));
                    put("country", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name", "surname"))
        );
        this.sqlgGraph.tx().commit();

        List<Vertex> persons = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertTrue(persons.isEmpty());
        try {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
            Assert.fail("Primary Key not specified, query suppose to fail!");
        } catch (Exception e) {
            //ignore
        }
        this.sqlgGraph.tx().rollback();

        this.sqlgGraph.addVertex(T.label, "Person", "name", "John", "surname", "Smith");
        this.sqlgGraph.tx().commit();
        persons = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(1, persons.size());
        Vertex person = persons.get(0);
        RecordId recordId = (RecordId) person.id();
        Assert.assertNull(recordId.getID().getSequenceId());
        Assert.assertEquals(2, recordId.getIdentifiers().size());
        Assert.assertEquals("John", recordId.getIdentifiers().get(0));
        Assert.assertEquals("Smith", recordId.getIdentifiers().get(1));
        Assert.assertEquals("John", person.property("name").value());
        Assert.assertEquals("Smith", person.property("surname").value());
        Assert.assertFalse(person.property("country").isPresent());

        person.property("country", "moon");
        this.sqlgGraph.tx().commit();
        person = this.sqlgGraph.traversal().V().hasLabel("Person").toList().get(0);
        Assert.assertTrue(person.property("country").isPresent());
        Assert.assertEquals("moon", person.value("country"));
    }

    @Test
    public void testEdgeInsertUpdate() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                this.sqlgGraph.getSqlDialect().getPublicSchema(),
                "A",
                new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(100)));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                this.sqlgGraph.getSqlDialect().getPublicSchema(),
                "B",
                new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(100)));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(100)));
                    put("country", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name"))
        );
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "A1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "A1");
        Edge e = a1.addEdge("ab", b1, "name", "halo", "country", "earth");
        this.sqlgGraph.tx().commit();

        List<Edge> edges = this.sqlgGraph.traversal().V().hasLabel("A").outE().toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals("earth", edges.get(0).value("country"));

        e.property("country", "moon");
        this.sqlgGraph.tx().commit();

        edges = this.sqlgGraph.traversal().V().hasLabel("A").outE().toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals("moon", edges.get(0).value("country"));
    }

    @Test
    public void testGetEdgeById() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                this.sqlgGraph.getSqlDialect().getPublicSchema(),
                "A",
                new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(100)));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                this.sqlgGraph.getSqlDialect().getPublicSchema(),
                "B",
                new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(100)));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(100)));
                    put("country", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name"))
        );
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "A1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "A1");
        Edge e = a1.addEdge("ab", b1, "name", "halo", "country", "earth");
        this.sqlgGraph.tx().commit();

        Vertex otherV = this.sqlgGraph.traversal().V(a1.id()).next();
        Assert.assertEquals(a1, otherV);
        Edge other = this.sqlgGraph.traversal().E(e.id()).next();
        Assert.assertEquals(e, other);
    }

}
