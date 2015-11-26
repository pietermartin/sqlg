package org.umlg.sqlg.test.mod;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Created by pieter on 2015/11/25.
 */
public class TestGremlinMod extends BaseTest {

    @Test
    public void testGremlinAddE() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        this.sqlgGraph.tx().commit();
        List<Edge> addedEdges =  this.sqlgGraph.traversal().V(a1).as("a").V(b1).addE("ab").from("a").toList();
        Assert.assertEquals(1, addedEdges.size());
    }
}
