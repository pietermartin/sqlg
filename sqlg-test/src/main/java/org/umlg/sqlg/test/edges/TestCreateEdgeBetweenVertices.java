package org.umlg.sqlg.test.edges;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2014/11/20
 * Time: 9:31 PM
 */
public class TestCreateEdgeBetweenVertices extends BaseTest {

    @Test
    public void testCreateEdgeBetweenVertices() {
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex person2 = this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.tx().commit();
        person1 = this.sqlgGraph.v(person1.id());
        person2 = this.sqlgGraph.v(person2.id());
        person1.addEdge("friend", person2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, vertexTraversal(person1).out("friend").count().next().intValue());
        Assert.assertEquals(1, vertexTraversal(person2).in("friend").count().next().intValue());
    }

    @Test
    public void testCreateEdgeBetweenVerticesPropertiesEagerlyLoadedOnHas() {
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Vertex person2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
        this.sqlgGraph.tx().commit();
        person1 = this.sqlgGraph.v(person1.id());
        person2 = this.sqlgGraph.v(person2.id());
        person1.addEdge("friend", person2);
        Assert.assertEquals("john", person1.value("name"));
        Assert.assertEquals("peter", person2.value("name"));
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().<Vertex>has(T.label, "Person").toList();
        Assert.assertEquals(1, vertexTraversal(vertices.get(0)).out("friend").count().next().intValue());
        Assert.assertEquals(1, vertexTraversal(vertices.get(1)).in("friend").count().next().intValue());
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testCreateEdgeBetweenVerticesPropertiesEagerlyLoadedOnHasHas() {
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Vertex person2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
        this.sqlgGraph.tx().commit();
        person1 = this.sqlgGraph.v(person1.id());
        person2 = this.sqlgGraph.v(person2.id());
        person1.addEdge("friend", person2);
        Assert.assertEquals("john", person1.value("name"));
        Assert.assertEquals("peter", person2.value("name"));
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().<Vertex>has(T.label, "Person").<Vertex>has("name", "john").toList();
        Assert.assertEquals(1, vertexTraversal(vertices.get(0)).out("friend").count().next().intValue());
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().<Vertex>has(T.label, "Person").<Vertex>has("name", "peter").toList();
        Assert.assertEquals(1, vertexTraversal(vertices.get(0)).in("friend").count().next().intValue());
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testCreateEdgeBetweenVerticesPropertiesEagerlyLoadedOnHasSortBy() {
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        for (int i = 0; i < 1000; i++) {
            Vertex person2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter" + i);
            person1.addEdge("friend", person2);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().<Vertex>has(T.label, "Person").toList();
        Assert.assertEquals("john", vertices.get(0).value("name"));
        Assert.assertEquals("peter0", vertices.get(1).value("name"));
        Assert.assertEquals("peter999", vertices.get(1000).value("name"));
    }

}
