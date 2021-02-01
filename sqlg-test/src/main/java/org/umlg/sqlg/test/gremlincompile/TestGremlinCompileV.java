package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2015/01/01
 * Time: 4:38 PM
 */
public class TestGremlinCompileV extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }


    @Test
    public void testSimpleOutOut() throws InterruptedException {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        a.addEdge("ab", b);
        this.sqlgGraph.tx().commit();
        testSimpleOutOut_assert(this.sqlgGraph, a);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testSimpleOutOut_assert(this.sqlgGraph1, a);
        }
    }

    private void testSimpleOutOut_assert(SqlgGraph sqlgGraph, Vertex a) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(sqlgGraph, a).out();
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testOutOut() throws InterruptedException {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C", "nAmE", "c");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "NAME", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "NAME", "d2");
        Vertex e = this.sqlgGraph.addVertex(T.label, "E", "NAME", "e");
        a.addEdge("outB", b);
        a.addEdge("outE", e);
        b.addEdge("outC", c);
        b.addEdge("outC", c);
        b.addEdge("outD", d1);
        b.addEdge("outD", d2);
        this.sqlgGraph.tx().commit();
        tetOutOut_assert(this.sqlgGraph, a, c, d1, d2);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            tetOutOut_assert(this.sqlgGraph1, a, c, d1, d2);
        }
    }

    private void tetOutOut_assert(SqlgGraph sqlgGraph, Vertex a, Vertex c, Vertex d1, Vertex d2) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(sqlgGraph, a).out().out();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(4, vertices.size());
        Assert.assertTrue(vertices.contains(c));
        Assert.assertTrue(vertices.contains(d1));
        Assert.assertTrue(vertices.contains(d2));
        int count = 0;
        for (Vertex vertex : vertices) {
            if (vertex.equals(c)) {
                count++;
            }
        }
        Assert.assertEquals(2, count);
        Assert.assertEquals("c", vertices.get(vertices.indexOf(c)).value("nAmE"));
        Assert.assertEquals("d1", vertices.get(vertices.indexOf(d1)).value("NAME"));
        Assert.assertEquals("d2", vertices.get(vertices.indexOf(d2)).value("NAME"));
    }

    @Test
    public void testOutOutWithLabels() throws InterruptedException {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C", "nAmE", "c");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "NAME", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "NAME", "d2");
        Vertex e = this.sqlgGraph.addVertex(T.label, "E", "NAME", "e");
        a.addEdge("outB", b);
        a.addEdge("outE", e);
        b.addEdge("outC", c);
        b.addEdge("outC", c);
        b.addEdge("outD", d1);
        b.addEdge("outD", d2);
        this.sqlgGraph.tx().commit();
        testOutOutWithLabels_assert(this.sqlgGraph, a, c, d1, d2);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testOutOutWithLabels_assert(this.sqlgGraph1, a, c, d1, d2);
        }
    }

    private void testOutOutWithLabels_assert(SqlgGraph sqlgGraph, Vertex a, Vertex c, Vertex d1, Vertex d2) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(sqlgGraph, a)
                .out("outB", "outE").out("outC", "outD");
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(4, vertices.size());
        Assert.assertTrue(vertices.contains(c));
        Assert.assertTrue(vertices.contains(d1));
        Assert.assertTrue(vertices.contains(d2));
        int count = 0;
        for (Vertex vertex : vertices) {
            if (vertex.equals(c)) {
                count++;
            }
        }
        Assert.assertEquals(2, count);
        Assert.assertEquals("c", vertices.get(vertices.indexOf(c)).value("nAmE"));
        Assert.assertEquals("d1", vertices.get(vertices.indexOf(d1)).value("NAME"));
        Assert.assertEquals("d2", vertices.get(vertices.indexOf(d2)).value("NAME"));
    }

    @Test
    public void testOutOutWithLabels2() throws InterruptedException {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C", "nAmE", "c");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "NAME", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "NAME", "d2");
        Vertex e = this.sqlgGraph.addVertex(T.label, "E", "NAME", "e");
        a.addEdge("outB", b);
        a.addEdge("outE", e);
        b.addEdge("outC", c);
        b.addEdge("outC", c);
        b.addEdge("outD", d1);
        b.addEdge("outD", d2);
        this.sqlgGraph.tx().commit();
        testOutOutWithLabels2_assert(this.sqlgGraph, a, c);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testOutOutWithLabels2_assert(this.sqlgGraph1, a, c);
        }
    }

    private void testOutOutWithLabels2_assert(SqlgGraph sqlgGraph, Vertex a, Vertex c) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(sqlgGraph, a).out("outB").out("outC");
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(c));
        int count = 0;
        for (Vertex vertex : vertices) {
            if (vertex.equals(c)) {
                count++;
            }
        }
        Assert.assertEquals(2, count);
        Assert.assertEquals("c", vertices.get(vertices.indexOf(c)).value("nAmE"));
    }

    @Test
    public void testInIn() throws InterruptedException {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C", "nAmE", "c");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "NAME", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "NAME", "d2");
        Vertex e = this.sqlgGraph.addVertex(T.label, "E", "NAME", "e");
        a.addEdge("outB", b);
        a.addEdge("outE", e);
        b.addEdge("outC", c);
        b.addEdge("outC", c);
        b.addEdge("outD", d1);
        b.addEdge("outD", d2);
        this.sqlgGraph.tx().commit();
        testInIn_assert(this.sqlgGraph, a, d1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testInIn_assert(this.sqlgGraph1, a, d1);
        }
    }

    private void testInIn_assert(SqlgGraph sqlgGraph, Vertex a, Vertex d1) {
        DefaultGraphTraversal<Vertex, Long> traversal = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(sqlgGraph, d1).in().in().count();
        Assert.assertEquals(4, traversal.getSteps().size());
        Assert.assertEquals(1, traversal.next().intValue());
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertEquals(a, vertexTraversal(sqlgGraph, d1).in().in().next());
    }

    @Test
    public void testInOutInOut() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        a1.addEdge("a_outB", b1);
        a1.addEdge("a_outB", b2);
        a1.addEdge("a_outB", b3);
        c1.addEdge("c_outB", b1);
        c2.addEdge("c_outB", b2);
        c3.addEdge("c_outB", b3);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(6, vertexTraversal(this.sqlgGraph, a1).out().in().count().next().intValue());

        Vertex e1 = this.sqlgGraph.addVertex(T.label, "E", "name", "e1");
        Vertex e2 = this.sqlgGraph.addVertex(T.label, "E", "name", "e2");
        Vertex e3 = this.sqlgGraph.addVertex(T.label, "E", "name", "e3");
        Vertex e4 = this.sqlgGraph.addVertex(T.label, "E", "name", "e4");
        Vertex e5 = this.sqlgGraph.addVertex(T.label, "E", "name", "e5");
        Vertex e6 = this.sqlgGraph.addVertex(T.label, "E", "name", "e6");
        Vertex e7 = this.sqlgGraph.addVertex(T.label, "E", "name", "e7");
        c1.addEdge("outE", e1);
        c2.addEdge("outE", e2);
        c2.addEdge("outE", e3);
        c2.addEdge("outE", e4);
        c3.addEdge("outE", e5);
        c3.addEdge("outE", e6);
        c3.addEdge("outE", e7);
        this.sqlgGraph.tx().commit();

        testInOutInOut_assert(this.sqlgGraph, a1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testInOutInOut_assert(this.sqlgGraph1, a1);
        }
    }

    private void testInOutInOut_assert(SqlgGraph sqlgGraph, Vertex a1) {
        DefaultGraphTraversal<Vertex, Long> traversal = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(sqlgGraph, a1).out().in().out().count();
        Assert.assertEquals(5, traversal.getSteps().size());
        Assert.assertEquals(19, traversal.next().intValue());
        Assert.assertEquals(3, traversal.getSteps().size());
    }

    @Test
    public void testInOutInOut3() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        Vertex e1 = this.sqlgGraph.addVertex(T.label, "E", "name", "e1");
        Vertex e2 = this.sqlgGraph.addVertex(T.label, "E", "name", "e2");
        Vertex e3 = this.sqlgGraph.addVertex(T.label, "E", "name", "e3");
        Vertex e4 = this.sqlgGraph.addVertex(T.label, "E", "name", "e4");
        Vertex e5 = this.sqlgGraph.addVertex(T.label, "E", "name", "e5");
        Vertex e6 = this.sqlgGraph.addVertex(T.label, "E", "name", "e6");
        Vertex e7 = this.sqlgGraph.addVertex(T.label, "E", "name", "e7");

        a1.addEdge("a_outB", b1);
        a1.addEdge("a_outB", b2);
        a1.addEdge("a_outB", b3);
        c1.addEdge("c_outB", b1);
        c2.addEdge("c_outB", b2);
        c3.addEdge("c_outB", b3);
        c1.addEdge("outE", e1);
        c2.addEdge("outE", e2);
        c2.addEdge("outE", e3);
        c2.addEdge("outE", e4);
        c3.addEdge("outE", e5);
        c3.addEdge("outE", e6);
        c3.addEdge("outE", e7);
        this.sqlgGraph.tx().commit();

        testInOutinOut3_assert(this.sqlgGraph, a1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testInOutinOut3_assert(this.sqlgGraph1, a1);
        }
    }

    private void testInOutinOut3_assert(SqlgGraph sqlgGraph, Vertex a1) {
        DefaultGraphTraversal<Vertex, Long> traversal = (DefaultGraphTraversal<Vertex, Long>) sqlgGraph.traversal().V(a1.id()).out().in().out().count();
        Assert.assertEquals(5, traversal.getSteps().size());
        Assert.assertEquals(19, traversal.next().intValue());
        Assert.assertEquals(3, traversal.getSteps().size());
    }

    @Test
    public void testInOutToSelf() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("knows", b1);
        b1.addEdge("knownBy", a2);

        //and another
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a3.addEdge("knows", b2);
        b2.addEdge("knownBy", a4);

        this.sqlgGraph.tx().commit();
        testInOutToSelf_assert(this.sqlgGraph, a1, a2);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testInOutToSelf_assert(this.sqlgGraph1, a1, a2);
        }
    }

    private void testInOutToSelf_assert(SqlgGraph sqlgGraph, Vertex a1, Vertex a2) {
        DefaultGraphTraversal<Vertex, Long> traversal = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(sqlgGraph, a1).out().out().count();
        Assert.assertEquals(4, traversal.getSteps().size());
        Assert.assertEquals(1, traversal.next().intValue());
        Assert.assertEquals(3, traversal.getSteps().size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(sqlgGraph, a1).out().out();
        Assert.assertEquals(3, traversal1.getSteps().size());
        Assert.assertEquals(a2, traversal1.next());
        Assert.assertEquals(1, traversal1.getSteps().size());
    }

    @Test
    public void testOutOutOutToSelf() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a1.addEdge("aOutB", b1);
        b1.addEdge("bOutC", c1);
        c1.addEdge("cOutB", b2);
        this.sqlgGraph.tx().commit();

        testOutOutoutToSelf_assert(this.sqlgGraph, a1, b2);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testOutOutoutToSelf_assert(this.sqlgGraph1, a1, b2);
        }
    }

    private void testOutOutoutToSelf_assert(SqlgGraph sqlgGraph, Vertex a1, Vertex b2) {
        DefaultGraphTraversal<Vertex, Long> traversal = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(sqlgGraph, a1).out().out().out().count();
        Assert.assertEquals(5, traversal.getSteps().size());
        Assert.assertEquals(1, traversal.next().intValue());
        Assert.assertEquals(3, traversal.getSteps().size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(sqlgGraph, a1).out().out().out();
        Assert.assertEquals(4, traversal1.getSteps().size());
        Assert.assertEquals(b2, traversal1.next());
        Assert.assertEquals(1, traversal1.getSteps().size());
    }

    @Test
    public void testOutInToSelf() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("aOutB", b1);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, vertexTraversal(this.sqlgGraph, a1).out().in().count().next().intValue());
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            Assert.assertEquals(1, vertexTraversal(this.sqlgGraph1, a1).out().in().count().next().intValue());
        }
    }

    @Test
    public void testInOutInOut2() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");

        a1.addEdge("a_outB", b1);
        a1.addEdge("a_outB", b2);
        a1.addEdge("a_outB", b3);

        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Long> traversal = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(this.sqlgGraph, a1).out().in().out().count();
        Assert.assertEquals(5, traversal.getSteps().size());
        Assert.assertEquals(9, traversal.next().intValue());
        Assert.assertEquals(3, traversal.getSteps().size());
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            DefaultGraphTraversal<Vertex, Long> traversal1 = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(this.sqlgGraph1, a1).out().in().out().count();
            Assert.assertEquals(5, traversal1.getSteps().size());
            Assert.assertEquals(9, traversal1.next().intValue());
            Assert.assertEquals(3, traversal1.getSteps().size());
        }
    }

    @Test
    public void testEmptyTraversal() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "B"); //        v1.addEdge("ab", v2);
        this.sqlgGraph.tx().commit();
        vertexTraversal(this.sqlgGraph, v1).out("test");
    }

    @Test
    public void testOutOutToSelf() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "ManagedObject", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "ManagedObject", "name", "a2");
        a1.addEdge("hierarchyParent_hierarchy", a2);
        this.sqlgGraph.tx().commit();
        testOutOutToSelf_assert(this.sqlgGraph, a1, a2);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testOutOutToSelf_assert(this.sqlgGraph1, a1, a2);
        }
    }

    private void testOutOutToSelf_assert(SqlgGraph sqlgGraph, Vertex a1, Vertex a2) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(sqlgGraph, a1).out();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.hasNext());
        Assert.assertEquals(1, traversal.getSteps().size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(sqlgGraph, a2).out();
        Assert.assertEquals(2, traversal1.getSteps().size());
        Assert.assertFalse(traversal1.hasNext());
        Assert.assertEquals(1, traversal1.getSteps().size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(sqlgGraph, a1).in();
        Assert.assertEquals(2, traversal2.getSteps().size());
        Assert.assertFalse(traversal2.hasNext());
        Assert.assertEquals(1, traversal2.getSteps().size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) vertexTraversal(sqlgGraph, a2).in();
        Assert.assertEquals(2, traversal3.getSteps().size());
        Assert.assertTrue(traversal3.hasNext());
        Assert.assertEquals(1, traversal3.getSteps().size());
    }

}
