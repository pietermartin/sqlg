package org.umlg.sqlg.test.mergestep;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;

public class TestMerge extends BaseTest {

    @Test
    public void testMergeWithDefaultLabel() {
        this.sqlgGraph.traversal().mergeV(new HashMap<>() {{
            put("name", "Brandy");
        }}).iterate();
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testMergeWithLabel() {
        this.sqlgGraph.traversal().mergeV(new HashMap<>() {{
            put(T.label, "Person");
            put("name", "Brandy");
        }}).iterate();
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("name", "Brandy").toList();
        Assert.assertEquals(1, vertices.size());
    }
}
