package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Date: 2014/07/22
 * Time: 7:53 PM
 */
public class TestEdgeToDifferentLabeledVertexes extends BaseTest {

    @Test
    public void testEdgeIsToDifferentLabeledVertexes() {
        Vertex v1 = this.sqlG.addVertex(T.label, "Person", "name", "a");
        Vertex v2 = this.sqlG.addVertex(T.label, "Person", "name", "b");
        Vertex v3 = this.sqlG.addVertex(T.label, "Product", "name", "c");
        v1.addEdge("label1", v2);
        v1.addEdge("label1", v3);
        this.sqlG.tx().commit();
        Assert.assertEquals(2, v1.out().count().next(), 0);
        Set<String> names = new HashSet();
        v1.out().forEach(
               v -> names.add(v.<String>property("name").value())
        );
        Assert.assertTrue(names.contains("b"));
        Assert.assertTrue(names.contains("c"));
    }
}
