package org.umlg.sqlg.test.gremlincompile;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

/**
 * Date: 2016/10/26
 * Time: 11:53 AM
 */
public class TestRepeatStepWithLabels extends BaseTest {

    @Test
    public void testEmitWithLabel() {

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1).repeat(out()).times(2).emit().as("bc").<Vertex>select("bc");
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> result = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.contains(b1));
        Assert.assertTrue(result.contains(c1));
    }

    @Test
    public void testRepeatEmitLabel1() {
        loadModern();
        DefaultGraphTraversal<Vertex, Map<String, Vertex>> traversal = (DefaultGraphTraversal<Vertex, Map<String, Vertex>>) this.sqlgGraph.traversal()
                .V().as("a")
                .repeat(out()).times(1).emit().as("b")
                .<Vertex>select("a", "b");
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Map<String, Vertex>> labelVertexMaps = traversal
                .toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Pair<Vertex, Vertex>> testPairs = new ArrayList<>();
        testPairs.add(Pair.of(convertToVertex(this.sqlgGraph, "marko"), convertToVertex(this.sqlgGraph, "lop")));
        testPairs.add(Pair.of(convertToVertex(this.sqlgGraph, "marko"), convertToVertex(this.sqlgGraph, "vadas")));
        testPairs.add(Pair.of(convertToVertex(this.sqlgGraph, "marko"), convertToVertex(this.sqlgGraph, "josh")));
        testPairs.add(Pair.of(convertToVertex(this.sqlgGraph, "josh"), convertToVertex(this.sqlgGraph, "ripple")));
        testPairs.add(Pair.of(convertToVertex(this.sqlgGraph, "josh"), convertToVertex(this.sqlgGraph, "lop")));
        testPairs.add(Pair.of(convertToVertex(this.sqlgGraph, "peter"), convertToVertex(this.sqlgGraph, "lop")));
        Assert.assertEquals(6, labelVertexMaps.size());
        for (Map<String, Vertex> labelVertexMap : labelVertexMaps) {
            Pair<Vertex, Vertex> pair = Pair.of(labelVertexMap.get("a"), labelVertexMap.get("b"));
            Assert.assertTrue(testPairs.remove(pair));
        }
        Assert.assertTrue(testPairs.isEmpty());
    }

    @Test
    public void testRepeatEmitLabel2() {
        loadModern();
        DefaultGraphTraversal<Vertex, Map<String, Vertex>> traversal = (DefaultGraphTraversal<Vertex, Map<String, Vertex>>) this.sqlgGraph.traversal()
                .V().as("a")
                .repeat(out()).times(2).emit().as("b")
                .<Vertex>select("a", "b");
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Map<String, Vertex>> labelVertexMaps = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());

        List<Pair<Vertex, Vertex>> testPairs = new ArrayList<>();
        testPairs.add(Pair.of(convertToVertex(this.sqlgGraph, "marko"), convertToVertex(this.sqlgGraph, "lop")));
        testPairs.add(Pair.of(convertToVertex(this.sqlgGraph, "marko"), convertToVertex(this.sqlgGraph, "lop")));
        testPairs.add(Pair.of(convertToVertex(this.sqlgGraph, "marko"), convertToVertex(this.sqlgGraph, "vadas")));
        testPairs.add(Pair.of(convertToVertex(this.sqlgGraph, "marko"), convertToVertex(this.sqlgGraph, "josh")));
        testPairs.add(Pair.of(convertToVertex(this.sqlgGraph, "marko"), convertToVertex(this.sqlgGraph, "ripple")));
        testPairs.add(Pair.of(convertToVertex(this.sqlgGraph, "josh"), convertToVertex(this.sqlgGraph, "ripple")));
        testPairs.add(Pair.of(convertToVertex(this.sqlgGraph, "josh"), convertToVertex(this.sqlgGraph, "lop")));
        testPairs.add(Pair.of(convertToVertex(this.sqlgGraph, "peter"), convertToVertex(this.sqlgGraph, "lop")));

        Assert.assertEquals(8, labelVertexMaps.size());
        for (Map<String, Vertex> labelVertexMap : labelVertexMaps) {
            System.out.println(labelVertexMap);
            Pair<Vertex, Vertex> pair = Pair.of(labelVertexMap.get("a"), labelVertexMap.get("b"));
            Assert.assertTrue(testPairs.remove(pair));
        }
        Assert.assertTrue(testPairs.isEmpty());
    }

}
