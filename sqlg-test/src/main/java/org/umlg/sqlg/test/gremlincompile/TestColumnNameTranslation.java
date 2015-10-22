package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.*;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2015/08/17
 * Time: 5:10 PM
 */
public class TestColumnNameTranslation extends BaseTest {

    @Test
    public void testInOutInOut2() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");

        a1.addEdge("a_outB", b1);
        a1.addEdge("a_outB", b2);
        a1.addEdge("a_outB", b3);

        this.sqlgGraph.tx().commit();

        assertEquals(9, vertexTraversal(a1).out().in().out().count().next().intValue());
    }

    @Test
    public void testNameWithMultipleSameLabel() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        a3.addEdge("ab", b3);
        this.sqlgGraph.tx().commit();

        List<Map<String, Object>> result = this.sqlgGraph.traversal()
                .V().as("a")
                .out().as("a")
                .in().as("a")
                .select("a", "a", "a")
                .toList();
        Assert.assertEquals(3, result.size());
        Object o1 = result.get(0).get("a");
        Assert.assertTrue(o1 instanceof List);
        List<Vertex> ass = (List) o1;
        Assert.assertEquals(a1, ass.get(0));
        Assert.assertEquals("a1", ass.get(0).value("name"));
        Assert.assertEquals(b1, ass.get(1));
        Assert.assertEquals("b1", ass.get(1).value("name"));
        Assert.assertEquals(a1, ass.get(2));
        Assert.assertEquals("a1", ass.get(2).value("name"));

    }

//    @Test
//    public void testShortName() {
//        Graph g = this.sqlgGraph;
//        Vertex a1 = g.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = g.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = g.addVertex(T.label, "B", "name", "b2");
//        Edge e1 = a1.addEdge("a_b", b1);
//        Edge e2 = a1.addEdge("a_b", b2);
//        Vertex c1 = g.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = g.addVertex(T.label, "C", "name", "c2");
//        Vertex c3 = g.addVertex(T.label, "C", "name", "c3");
//        Edge bc1 = b1.addEdge("b_c", c1);
//        Edge bc2 = b1.addEdge("b_c", c2);
//        Edge bc3 = b1.addEdge("b_c", c3);
//
//        Vertex d1 = g.addVertex(T.label, "D", "name", "d1");
//        Vertex d2 = g.addVertex(T.label, "D", "name", "d2");
//        Vertex d3 = g.addVertex(T.label, "D", "name", "d3");
//
//        Edge bd1 = b1.addEdge("b_d", d1);
//        Edge bd2 = b1.addEdge("b_d", d2);
//        Edge bd3 = b1.addEdge("b_d", d3);
//
//        this.sqlgGraph.tx().commit();
//
//        List<Map<String, Object>> result = g.traversal().V(a1)
//                .outE("a_b").as("ab")
//                .inV().as("B")
//                .outE().as("bcd")
//                .inV().as("CD")
//                .inE().as("dcb")
//                .inV().as("CD2")
//                .select("ab", "B", "bcd", "CD", "dcb", "CD2").toList();
//        Assert.assertFalse(result.isEmpty());
//        Assert.assertEquals(6, result.size());
//
//        Assert.assertEquals(e1, result.get(0).get("ab"));
//        Assert.assertEquals(e1, result.get(1).get("ab"));
//        Assert.assertEquals(e1, result.get(2).get("ab"));
//        Assert.assertEquals(e1, result.get(3).get("ab"));
//        Assert.assertEquals(e1, result.get(4).get("ab"));
//        Assert.assertEquals(e1, result.get(5).get("ab"));
//
//        Assert.assertEquals(b1, result.get(0).get("B"));
//        Assert.assertEquals("b1", ((Vertex) result.get(0).get("B")).value("name"));
//        Assert.assertEquals(b1, result.get(1).get("B"));
//        Assert.assertEquals("b1", ((Vertex) result.get(1).get("B")).value("name"));
//        Assert.assertEquals(b1, result.get(2).get("B"));
//        Assert.assertEquals("b1", ((Vertex) result.get(2).get("B")).value("name"));
//        Assert.assertEquals(b1, result.get(3).get("B"));
//        Assert.assertEquals("b1", ((Vertex) result.get(3).get("B")).value("name"));
//        Assert.assertEquals(b1, result.get(4).get("B"));
//        Assert.assertEquals("b1", ((Vertex) result.get(4).get("B")).value("name"));
//        Assert.assertEquals(b1, result.get(5).get("B"));
//        Assert.assertEquals("b1", ((Vertex) result.get(5).get("B")).value("name"));
//
//        List<Edge> bcds = new ArrayList<>(Arrays.asList(bc1, bc2, bc3, bd1, bd2, bd3));
//        Assert.assertTrue(bcds.contains(result.get(0).get("bcd")));
//        bcds.remove(result.get(0).get("bcd"));
//        Assert.assertTrue(bcds.contains(result.get(1).get("bcd")));
//        bcds.remove(result.get(1).get("bcd"));
//        Assert.assertTrue(bcds.contains(result.get(2).get("bcd")));
//        bcds.remove(result.get(2).get("bcd"));
//        Assert.assertTrue(bcds.contains(result.get(3).get("bcd")));
//        bcds.remove(result.get(3).get("bcd"));
//        Assert.assertTrue(bcds.contains(result.get(4).get("bcd")));
//        bcds.remove(result.get(4).get("bcd"));
//        Assert.assertTrue(bcds.contains(result.get(5).get("bcd")));
//        bcds.remove(result.get(5).get("bcd"));
//        Assert.assertTrue(bcds.isEmpty());
//
//        List<Vertex> cds = new ArrayList<>(Arrays.asList(c1, c2, c3, d1, d2, d3));
//        List<String> cdsNames = new ArrayList<>(Arrays.asList("c1", "c2", "c3", "d1", "d2", "d3"));
//        Assert.assertTrue(cds.contains(result.get(0).get("CD")));
//        cds.remove(result.get(0).get("CD"));
//        Assert.assertTrue(cdsNames.contains(((Vertex) result.get(0).get("CD")).value("name")));
//        cdsNames.remove(((Vertex) result.get(0).get("CD")).value("name"));
//
//        Assert.assertTrue(cds.contains(result.get(1).get("CD")));
//        cds.remove(result.get(1).get("CD"));
//        Assert.assertTrue(cdsNames.contains(((Vertex) result.get(1).get("CD")).value("name")));
//        cdsNames.remove(((Vertex) result.get(1).get("CD")).value("name"));
//
//        Assert.assertTrue(cds.contains(result.get(2).get("CD")));
//        cds.remove(result.get(2).get("CD"));
//        Assert.assertTrue(cdsNames.contains(((Vertex) result.get(2).get("CD")).value("name")));
//        cdsNames.remove(((Vertex) result.get(2).get("CD")).value("name"));
//
//        Assert.assertTrue(cds.contains(result.get(3).get("CD")));
//        cds.remove(result.get(3).get("CD"));
//        Assert.assertTrue(cdsNames.contains(((Vertex) result.get(3).get("CD")).value("name")));
//        cdsNames.remove(((Vertex) result.get(3).get("CD")).value("name"));
//
//        Assert.assertTrue(cds.contains(result.get(4).get("CD")));
//        cds.remove(result.get(4).get("CD"));
//        Assert.assertTrue(cdsNames.contains(((Vertex) result.get(4).get("CD")).value("name")));
//        cdsNames.remove(((Vertex) result.get(4).get("CD")).value("name"));
//
//        Assert.assertTrue(cds.contains(result.get(5).get("CD")));
//        cds.remove(result.get(5).get("CD"));
//        Assert.assertTrue(cdsNames.contains(((Vertex) result.get(5).get("CD")).value("name")));
//        cdsNames.remove(((Vertex) result.get(5).get("CD")).value("name"));
//        Assert.assertTrue(cds.isEmpty());
//        Assert.assertTrue(cdsNames.isEmpty());
//
//        bcds = new ArrayList<>(Arrays.asList(bc1, bc2, bc3, bd1, bd2, bd3));
//        Assert.assertTrue(bcds.contains(result.get(0).get("dcb")));
//        bcds.remove(result.get(0).get("dcb"));
//        Assert.assertTrue(bcds.contains(result.get(1).get("dcb")));
//        bcds.remove(result.get(1).get("dcb"));
//        Assert.assertTrue(bcds.contains(result.get(2).get("dcb")));
//        bcds.remove(result.get(2).get("dcb"));
//        Assert.assertTrue(bcds.contains(result.get(3).get("dcb")));
//        bcds.remove(result.get(3).get("dcb"));
//        Assert.assertTrue(bcds.contains(result.get(4).get("dcb")));
//        bcds.remove(result.get(4).get("dcb"));
//        Assert.assertTrue(bcds.contains(result.get(5).get("dcb")));
//        bcds.remove(result.get(5).get("dcb"));
//        Assert.assertTrue(bcds.isEmpty());
//
//        cds = new ArrayList<>(Arrays.asList(c1, c2, c3, d1, d2, d3));
//        Assert.assertTrue(cds.contains(result.get(0).get("CD2")));
//        cds.remove(result.get(0).get("CD2"));
//        Assert.assertTrue(cds.contains(result.get(1).get("CD2")));
//        cds.remove(result.get(1).get("CD2"));
//        Assert.assertTrue(cds.contains(result.get(2).get("CD2")));
//        cds.remove(result.get(2).get("CD2"));
//        Assert.assertTrue(cds.contains(result.get(3).get("CD2")));
//        cds.remove(result.get(3).get("CD2"));
//        Assert.assertTrue(cds.contains(result.get(4).get("CD2")));
//        cds.remove(result.get(4).get("CD2"));
//        Assert.assertTrue(cds.contains(result.get(5).get("CD2")));
//        cds.remove(result.get(5).get("CD2"));
//        Assert.assertTrue(cds.isEmpty());
//    }
//
//    @Test
//    public void testLongName() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "AAAAAAAAAAAAAAAAAAAAAAAAA");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "BBBBBBBBBBBBBBBBBBBBBBBBB");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "BBBBBBBBBBBBBBBBBBBBBBBBB");
//        Edge e1 = a1.addEdge("aaaaaaaaaaaaaaaaaaaaaa_bbbbbbbbbbbbbbbbbbbb", b1);
//        Edge e2 = a1.addEdge("aaaaaaaaaaaaaaaaaaaaaa_bbbbbbbbbbbbbbbbbbbb", b2);
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "CCCCCCCCCCCCCCCCCCCCCCCCC");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "CCCCCCCCCCCCCCCCCCCCCCCCC");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "CCCCCCCCCCCCCCCCCCCCCCCCC");
//        b1.addEdge("bbbbbbbbbbbbbbbbbbbbbb_cccccccccccccccc", c1);
//        b1.addEdge("bbbbbbbbbbbbbbbbbbbbbb_cccccccccccccccc", c2);
//        b1.addEdge("bbbbbbbbbbbbbbbbbbbbbb_cccccccccccccccc", c3);
//
//        Vertex d1 = this.sqlgGraph.addVertex(T.label, "DDDDDDDDDDDDDDDDDDDDDDDDD");
//        Vertex d2 = this.sqlgGraph.addVertex(T.label, "DDDDDDDDDDDDDDDDDDDDDDDDD");
//        Vertex d3 = this.sqlgGraph.addVertex(T.label, "DDDDDDDDDDDDDDDDDDDDDDDDD");
//
//        b1.addEdge("bbbbbbbbbbbbbbbbbbbbbb_dddddddddddddddd", d1);
//        b1.addEdge("bbbbbbbbbbbbbbbbbbbbbb_dddddddddddddddd", d2);
//        b1.addEdge("bbbbbbbbbbbbbbbbbbbbbb_dddddddddddddddd", d3);
//
//        this.sqlgGraph.tx().commit();
//        List<Map<String, Object>> gt = this.sqlgGraph.traversal().V(a1)
//                .outE("aaaaaaaaaaaaaaaaaaaaaa_bbbbbbbbbbbbbbbbbbbb").as("ab")
//                .inV().as("B")
//                .outE().as("bcd")
//                .inV().as("CD")
//                .inE().as("INE")
//                .inV().as("CD2")
//                .select("ab", "B", "bcd", "CD", "INE", "CD2").toList();
//        Assert.assertFalse(gt.isEmpty());
//    }
}
