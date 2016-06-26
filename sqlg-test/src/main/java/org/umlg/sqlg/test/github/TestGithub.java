package org.umlg.sqlg.test.github;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/04/26
 * Time: 5:02 PM
 */
public class TestGithub extends BaseTest {

//    @Test
    public void edgeUpdate() {
        Vertex a = this.sqlgGraph.addVertex("A");
        Vertex b = this.sqlgGraph.addVertex("B");
        Edge a2b = a.addEdge("a2b", b);
        a2b.property("someKey", "someValue");

        Edge found_a2b = this.sqlgGraph.traversal().E().has("someKey", "someValue").next();
        found_a2b.property("anotherKey", "anotherValue");

        this.sqlgGraph.tx().commit();

        assertEquals("someValue", found_a2b.property("someKey").value());
        assertEquals("anotherValue", found_a2b.property("anotherKey").value());
        assertEquals("someValue", a2b.property("someKey").value());
//        assertEquals("anotherValue", a2b.property("anotherKey").value());
    }

    @Test
    public void testStaleEdge() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a1Again = this.sqlgGraph.traversal().V().hasLabel("A").next();
        this.sqlgGraph.tx().commit();
        a1Again.property("nameAgain", "a1");
        this.sqlgGraph.tx().commit();
        assertEquals("a1", a1.property("name").value());
        assertEquals("a1", a1.property("nameAgain").value());
    }
}
