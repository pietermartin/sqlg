package org.umlg.sqlg.test.vertex;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2014/10/25
 * Time: 12:46 PM
 */
public class TestPropertyOnRemovedVertex extends BaseTest {

    @Test
    public void shouldReturnEmptyPropertyIfVertexWasRemoved() {
        final Vertex v1 = this.sqlgGraph.addVertex("name", "stephen");
        v1.remove();
        this.sqlgGraph.tx().commit();
        assertEquals(VertexProperty.empty(), v1.property("name"));
    }

    @Test
    public void shouldReturnEmptyPropertyIfEdgeWasRemoved() {
        final Vertex v1 = this.sqlgGraph.addVertex("name", "stephen");
        final Edge e = v1.addEdge("knows", v1, "x", "y");
        e.remove();
        sqlgGraph.tx().commit();
        assertEquals(Property.empty(), e.property("x"));
    }
}
