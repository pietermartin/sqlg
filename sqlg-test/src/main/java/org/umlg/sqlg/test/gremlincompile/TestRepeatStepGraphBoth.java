package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;

/**
 * Date: 2015/10/21
 * Time: 8:18 PM
 */
public class TestRepeatStepGraphBoth extends BaseTest {

    @Test
    public void testRepeatBoth() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        List<Map<String, Vertex>> vertexList = this.sqlgGraph.traversal()
                .V(b1).as("a")
                .repeat(both()).times(3).emit().as("b")
                .<Vertex>select("a", "b")
                .toList();

        Assert.assertEquals(8, vertexList.size());

        List<Vertex> aVertices = new ArrayList<>();
        List<Vertex> bVertices = new ArrayList<>();
        for (Map<String, Vertex> stringVertexMap : vertexList) {
            System.out.println(stringVertexMap);
            Assert.assertEquals(2, stringVertexMap.size());
            aVertices.add(stringVertexMap.get("a"));
            bVertices.add(stringVertexMap.get("b"));
        }
        Assert.assertTrue(aVertices.remove(b1));
        Assert.assertTrue(aVertices.remove(b1));
        Assert.assertTrue(aVertices.remove(b1));
        Assert.assertTrue(aVertices.remove(b1));
        Assert.assertTrue(aVertices.remove(b1));
        Assert.assertTrue(aVertices.remove(b1));
        Assert.assertTrue(aVertices.remove(b1));
        Assert.assertTrue(aVertices.remove(b1));
        Assert.assertTrue(aVertices.isEmpty());

        Assert.assertTrue(bVertices.remove(c1));
        Assert.assertTrue(bVertices.remove(c1));
        Assert.assertTrue(bVertices.remove(c1));
        Assert.assertTrue(bVertices.remove(a1));
        Assert.assertTrue(bVertices.remove(a1));
        Assert.assertTrue(bVertices.remove(a1));
        Assert.assertTrue(bVertices.remove(b1));
        Assert.assertTrue(bVertices.remove(b1));
        Assert.assertTrue(bVertices.isEmpty());
    }

    @Test
    public void testGroupByByFailure() {
        loadModern();
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Collection<Vertex>> traversal = this.sqlgGraph.traversal()
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
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(6, vertices.size());
        Assert.assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "marko")));
        Assert.assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "vadas")));
        Assert.assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "josh")));
        Assert.assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "peter")));
        Assert.assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "lop")));
        Assert.assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "ripple")));
    }

    @Test
    public void testRepeatAndOut() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().repeat(both()).times(1).out().toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(b1, vertices.get(0));
    }

    @Test
    public void testRepeatBoth2() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V(b1).repeat(both()).times(3).toList();
        Assert.assertEquals(4, vertices.size());
    }

    @Test
    public void testRepeatBothE() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V(b1).repeat(__.bothE().otherV()).times(3).toList();
        Assert.assertEquals(4, vertices.size());
    }

    @Test
    public void testRepeatBothEWithAggregate() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Edge e1 = a1.addEdge("ab", b1);
        Edge e2 = b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V(b1).repeat(__.bothE().where(P.without("e")).aggregate("e").otherV()).emit().path().toList();
        Assert.assertEquals(2, paths.size());

        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(b1) && p.get(1).equals(e1) && p.get(2).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(b1) && p.get(1).equals(e1) && p.get(2).equals(a1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(b1) && p.get(1).equals(e2) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(b1) && p.get(1).equals(e2) && p.get(2).equals(c1)).findAny().get());
        Assert.assertTrue(paths.isEmpty());
    }
}
