package org.umlg.sqlg.test.labels;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertEquals;

/**
 * Test label behaviors when we use schema.label
 *
 * @author jpmoresmau
 */
public class TestLabelsSchema extends BaseTest {

    @Test
    public void testLabelSchema() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "S1.A");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "S2.B");
        assertEquals("S1.A", v1.label());
        assertEquals("S2.B", v2.label());
    }

    @Test
    public void testOrHasSchema() {
        this.sqlgGraph.addVertex(T.label, "S1.A");
        this.sqlgGraph.addVertex(T.label, "S2.B");
        assertEquals(2L, this.sqlgGraph.traversal().V().or(
                __.hasLabel("S1.A")
                , __.hasLabel("S2.B")
        ).count().next().longValue());
    }

    @Test
    public void testHasLabelSchema() {
        this.sqlgGraph.addVertex(T.label, "S1.A");
        this.sqlgGraph.addVertex(T.label, "S2.B");
        assertEquals(1L, this.sqlgGraph.traversal().V().hasLabel("S1.A")
                .count().next().longValue());
        assertEquals(1L, this.sqlgGraph.traversal().V().has(T.label, P.eq("S1.A"))
                .count().next().longValue());
        assertEquals(1L, this.sqlgGraph.traversal().V().has(T.label, P.within("S1.A"))
                .count().next().longValue());
        assertEquals(1L, this.sqlgGraph.traversal().V().hasLabel("S2.B")
                .count().next().longValue());
    }

    /**
     * Edge labels are NOT prefixed by schema
     */
    @Test
    public void testLabelSchemaEdge() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "S1.A");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "S2.B");
        Edge e1 = v1.addEdge("E1", v2, "name", "e1");
        assertEquals("E1", e1.label());
        assertEquals(1L, this.sqlgGraph.traversal().E().hasLabel("E1")
                .count().next().longValue());
        assertEquals(1L, this.sqlgGraph.traversal().E().has(T.label, P.eq("E1"))
                .count().next().longValue());
    }

    /**
     * Edge labels are NOT prefixed by schema
     */
    @Test
    public void testMultipleLabelSchemaEdge() {
        Vertex aa = this.sqlgGraph.addVertex(T.label, "A.A");
        Vertex ab = this.sqlgGraph.addVertex(T.label, "A.B");
        Vertex ba = this.sqlgGraph.addVertex(T.label, "B.A");
        Vertex bb = this.sqlgGraph.addVertex(T.label, "B.B");
        aa.addEdge("ab", ab);
        ba.addEdge("ab", bb);
        this.sqlgGraph.tx().commit();
        assertEquals(2L, this.sqlgGraph.traversal().E().hasLabel("ab").count().next().longValue());
    }
}
