package org.umlg.sqlg.test.mod;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.*;

/**
 * Date: 2014/08/28
 * Time: 7:14 AM
 */
public class TestUpdateVertex extends BaseTest {

    @Test
    public void testUpdateVertex() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        assertEquals("john", v.value("name"));
        v.property("name", "joe");
        assertEquals("joe", v.value("name"));
        this.sqlgGraph.tx().commit();
        assertEquals("joe", v.value("name"));
    }

    @Test
    public void testPropertyIsPresent() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        assertTrue(v.property("name").isPresent());
    }

    @Test
    public void testLoadPropertiesOnUpdate() {
        Vertex vertex = this.sqlgGraph.addVertex(T.label, "Person", "property1", "a", "property2", "b");
        this.sqlgGraph.tx().commit();

        vertex = this.sqlgGraph.traversal().V(vertex.id()).next();
        vertex.property("property1", "aa");
        assertEquals("b", vertex.value("property2"));

    }

    @Test
    public void testUpdatePropertyWithPeriod() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "test.A", "test");
        this.sqlgGraph.tx().commit();
        v = this.sqlgGraph.traversal().V(v).next();
        v.property("test.A", "test1");
        this.sqlgGraph.tx().commit();
        v = this.sqlgGraph.traversal().V(v).next();
        assertEquals("test1", v.property("test.A").value());
    }

    @Test
    public void testUpdateStringArray() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "ManagedObject", "source", new String[]{"MML"});
        this.sqlgGraph.tx().commit();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        assertArrayEquals(new String[]{"MML"}, v1.value("source"));

        v1.property("source", new String[]{"XML"});
        this.sqlgGraph.tx().commit();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        assertArrayEquals(new String[]{"XML"}, v1.value("source"));
    }

}
