package org.umlg.sqlg.test.edges;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Created by pieter on 2015/07/05.
 */
public class TestLoadEdge extends BaseTest {

    @Test
    public void testEdgeInOutVerticesUserDefinedPrimary() {
        VertexLabel personVertexLabel1 = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                this.sqlgGraph.getSqlDialect().getPublicSchema(),
                "Person",
                new LinkedHashMap<>() {{
                    put("id1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("id2", PropertyDefinition.of(PropertyType.INTEGER));
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("id1", "id2"))
        );
        VertexLabel addressVertexLabel1 = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                this.sqlgGraph.getSqlDialect().getPublicSchema(),
                "Address",
                new LinkedHashMap<>() {{
                    put("addressId1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("addressId2", PropertyDefinition.of(PropertyType.INTEGER));
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("addressId1", "addressId2"))
        );
        VertexLabel carVertexLabel1 = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                this.sqlgGraph.getSqlDialect().getPublicSchema(),
                "Car",
                new LinkedHashMap<>() {{
                    put("carId1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("carId2", PropertyDefinition.of(PropertyType.INTEGER));
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("carId1", "carId2"))
        );
        this.sqlgGraph.tx().commit();
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "id1", 1, "id2", 1, "name", "John");
        Vertex address1 = this.sqlgGraph.addVertex(T.label, "Address", "addressId1", 1, "addressId2", 1, "name", "John");
        Vertex car1 = this.sqlgGraph.addVertex(T.label, "Car", "carId1", 1, "carId2", 1, "name", "John");
        Edge e1 = person1.addEdge("edge", address1);
        Edge e2 = person1.addEdge("edge", car1);
        this.sqlgGraph.tx().commit();

        e1.property("what", "this");
        e2.property("what", "this");

        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").out("edge").toList();
        Assert.assertEquals(2, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("Car").in("edge").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("Address").in("edge").toList();
        Assert.assertEquals(1, vertices.size());

        List<String> values = this.sqlgGraph.traversal().E().hasLabel("edge").<String>values("what").toList();
        Assert.assertEquals(2, values.size());
        Assert.assertEquals("this", values.get(0));
        Assert.assertEquals("this", values.get(1));
    }


    @Test
    public void testEdgeInOutVertices() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Edge e1 = a1.addEdge("edge1", b1);
        Edge e2 = a1.addEdge("edge1", c1);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(a1, e1.outVertex());
        Assert.assertEquals(b1, e1.inVertex());
        e1.property("what", "this");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(a1, e1.outVertex());
        Assert.assertEquals(b1, e1.inVertex());
    }

    @Test
    public void testLoadEdge() {
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex person2 = this.sqlgGraph.addVertex(T.label, "Person");
        Edge friend = person1.addEdge("friend", person2, "name", "edge1");
        this.sqlgGraph.tx().commit();
        Property<?> p = this.sqlgGraph.traversal().E(friend).next().property("name", "edge2");
        Assert.assertNotNull(p.value());
    }

    @Test
    public void testEdgePropertyWithPeriod() {
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex person2 = this.sqlgGraph.addVertex(T.label, "Person");
        Edge friend = person1.addEdge("friend", person2, "name.A", "edge1");
        this.sqlgGraph.tx().commit();
        Property<?> p = this.sqlgGraph.traversal().E(friend).next().property("name.A");
        Assert.assertNotNull(p.value());
        Assert.assertEquals("edge1", p.value());
    }

    @Test
    public void shouldConstructDetachedEdge() {
        Graph g = this.sqlgGraph;
        loadModern();
        assertModernGraph(g, true, false);
        Edge e = g.traversal().E(convertToEdgeId("marko", "knows", "vadas")).next();
        e.property("year", 2002);
        g.tx().commit();
        e = g.traversal().E(convertToEdgeId("marko", "knows", "vadas")).next();
        final DetachedEdge detachedEdge = DetachedFactory.detach(e, true);
        Assert.assertEquals(convertToEdgeId("marko", "knows", "vadas"), detachedEdge.id());

        Assert.assertEquals("knows", detachedEdge.label());
        Assert.assertEquals(DetachedVertex.class, detachedEdge.vertices(Direction.OUT).next().getClass());
        Assert.assertEquals(convertToVertexId("marko"), detachedEdge.vertices(Direction.OUT).next().id());
        Assert.assertEquals("person", detachedEdge.vertices(Direction.IN).next().label());
        Assert.assertEquals(DetachedVertex.class, detachedEdge.vertices(Direction.IN).next().getClass());
        Assert.assertEquals(convertToVertexId("vadas"), detachedEdge.vertices(Direction.IN).next().id());
        Assert.assertEquals("person", detachedEdge.vertices(Direction.IN).next().label());

        Assert.assertEquals(2, IteratorUtils.count(detachedEdge.properties()));
        Assert.assertEquals(1, IteratorUtils.count(detachedEdge.properties("year")));
        Assert.assertEquals(0.5d, detachedEdge.properties("weight").next().value());
    }

    private Object convertToEdgeId(final String outVertexName, String edgeLabel, final String inVertexName) {
        return this.sqlgGraph.traversal().V().has("name", outVertexName).outE(edgeLabel).as("e").inV().has("name", inVertexName).<Edge>select("e").next().id();
    }
}
