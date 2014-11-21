package org.umlg.sqlg.test.edges;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

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
        Assert.assertEquals(1, person1.out("friend").count().next().intValue());
        Assert.assertEquals(1, person2.in("friend").count().next().intValue());
    }

    @Test
    public void testCreateEdgeBetweenVerticesPropertiesEagerlyLoaded() {
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Vertex person2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
        this.sqlgGraph.tx().commit();
        person1 = this.sqlgGraph.v(person1.id());
        person2 = this.sqlgGraph.v(person2.id());
        Assert.assertEquals("john", person1.value("name"));
        Assert.assertEquals("peter", person2.value("name"));
        this.sqlgGraph.tx().commit();
    }
}
