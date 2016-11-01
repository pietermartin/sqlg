package org.umlg.sqlg.test.vertex;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Lukas Krejci
 */
public class TestLoadingAdjacent extends BaseTest {

    @Test
    public void testLoadAdjacentVertices() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john1");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john2");
        v1.addEdge("friend", v2, "weight", 1);
        this.sqlgGraph.tx().commit();

        Iterator<Vertex> adjacents = v1.vertices(Direction.OUT);

        Assert.assertTrue(adjacents.hasNext());

        Vertex adjacent = adjacents.next();

        Assert.assertFalse(adjacents.hasNext());

        Assert.assertEquals(v2.id(), adjacent.id());
        Assert.assertEquals(toList(v2.properties()), toList(adjacent.properties()));
    }

    @Test
    public void testLoadEdges() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john1");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john2");
        Edge e = v1.addEdge("friend", v2, "weight", 1);
        this.sqlgGraph.tx().commit();

        Iterator<Edge> adjacents = v1.edges(Direction.OUT);

        Assert.assertTrue(adjacents.hasNext());

        Edge adjacent = adjacents.next();

        Assert.assertFalse(adjacents.hasNext());

        Assert.assertEquals(e.id(), adjacent.id());
        Assert.assertEquals(toList(e.properties()), toList(adjacent.properties()));
    }

    private static <T>List<T> toList(Iterator<T> it) {
        ArrayList<T> ret = new ArrayList<>();
        while (it.hasNext()) ret.add(it.next());
        return ret;
    }
}
