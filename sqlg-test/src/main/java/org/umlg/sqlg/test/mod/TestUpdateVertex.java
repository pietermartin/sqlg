package org.umlg.sqlg.test.mod;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/08/28
 * Time: 7:14 AM
 */
public class TestUpdateVertex extends BaseTest {

    @Test
    public void testUpdateVertex() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Assert.assertEquals("john", v.value("name"));
        v.property("name", "joe");
        Assert.assertEquals("joe", v.value("name"));
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("joe", v.value("name"));
    }

    @Test
    public void testPropertyIsPresent() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Assert.assertTrue(v.property("name").isPresent());
    }

    @Test
    public void testLoadPropertiesOnUpdate() {
        Vertex vertex = this.sqlgGraph.addVertex(T.label, "Person", "property1", "a", "property2", "b");
        this.sqlgGraph.tx().commit();

        vertex = this.sqlgGraph.v(vertex.id());
        vertex.property("property1", "aa");
        Assert.assertEquals("b", vertex.value("property2"));

    }

    @Test
    public void testUpdatePropertyWithPeriod() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "test.A", "test");
        this.sqlgGraph.tx().commit();
        v = this.sqlgGraph.traversal().V(v).next();
        v.property("test.A", "test1");
        this.sqlgGraph.tx().commit();
        v = this.sqlgGraph.traversal().V(v).next();
        Assert.assertEquals("test1", v.property("test.A").value());
    }

}
