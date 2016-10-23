package org.umlg.sqlg.test.dedup;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Collection;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2016/10/18
 * Time: 7:49 AM
 */
public class TinkerPopDedupTest extends BaseTest {

    @Test
    public void testDedupFailure() {
        loadModern();
        this.sqlgGraph.tx().commit();

        final Traversal<Vertex, Collection<Vertex>> traversal = this.sqlgGraph.traversal()
                .V().as("a")
                .repeat(both()).times(3).emit().as("b")
                .group()
                .by(select("a"))
                .by(select("b").dedup().order().by(T.id).fold())
                .select(Column.values)
                .<Collection<Vertex>>unfold()
                .dedup();
        printTraversalForm(traversal);

        final Collection<Vertex> vertices = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(6, vertices.size());
        assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "marko")));
        assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "vadas")));
        assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "josh")));
        assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "peter")));
        assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "lop")));
        assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "ripple")));

    }
}
