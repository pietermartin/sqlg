package org.umlg.sqlg.test.gremlincompile;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2015/01/01
 * Time: 4:38 PM
 */
public class TestGremlinCompileV extends BaseTest {

    @Test
    public void testOutOut() {
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
        List<Vertex> vertices = a.out().out().toList();
        assertEquals(4, vertices.size());
        assertTrue(vertices.contains(c));
        assertTrue(vertices.contains(d1));
        assertTrue(vertices.contains(d2));
        int count = 0;
        for (Vertex vertex : vertices) {
            if (vertex.equals(c)) {
                count++;
            }
        }
        assertEquals(2, count);
        assertEquals("c", vertices.get(vertices.indexOf(c)).value("nAmE"));
        assertEquals("d1", vertices.get(vertices.indexOf(d1)).value("NAME"));
        assertEquals("d2", vertices.get(vertices.indexOf(d2)).value("NAME"));
    }

    @Test
    public void testOutOutWithLabels() {
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
        List<Vertex> vertices = a.out("outB", "outE").out("outC", "outD").toList();
        assertEquals(4, vertices.size());
        assertTrue(vertices.contains(c));
        assertTrue(vertices.contains(d1));
        assertTrue(vertices.contains(d2));
        int count = 0;
        for (Vertex vertex : vertices) {
            if (vertex.equals(c)) {
                count++;
            }
        }
        assertEquals(2, count);
        assertEquals("c", vertices.get(vertices.indexOf(c)).value("nAmE"));
        assertEquals("d1", vertices.get(vertices.indexOf(d1)).value("NAME"));
        assertEquals("d2", vertices.get(vertices.indexOf(d2)).value("NAME"));
    }

    @Test
    public void testOutOutWithLabels2() {
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
        List<Vertex> vertices = a.out("outB").out("outC").toList();
        assertEquals(2, vertices.size());
        assertTrue(vertices.contains(c));
        int count = 0;
        for (Vertex vertex : vertices) {
            if (vertex.equals(c)) {
                count++;
            }
        }
        assertEquals(2, count);
        assertEquals("c", vertices.get(vertices.indexOf(c)).value("nAmE"));
    }
    @Test
    public void testInIn() {
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
        assertEquals(1, d1.in().in().count().next().intValue());
        assertEquals(a, d1.in().in().next());
    }

    @Test
    public void testInOutInOut() {
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
        assertEquals(6, a1.out().in().count().next().intValue());

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

        assertEquals(19, a1.out().in().out().count().next().intValue());
    }

    @Test
    public void testInOutInOut3() {
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

        assertEquals(19, a1.out().in().out().count().next().intValue());
    }

    @Test
    public void testInOutToSelf() {

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
        assertEquals(1, a1.out().out().count().next().intValue());
        assertEquals(a2, a1.out().out().next());
    }

    @Test
    public void testOutOutOutToSelf() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a1.addEdge("aOutB", b1);
        b1.addEdge("bOutC", c1);
        c1.addEdge("cOutB", b2);
        this.sqlgGraph.tx().commit();

        assertEquals(1, a1.out().out().out().count().next().intValue());
        assertEquals(b2, a1.out().out().out().next());
    }

    @Test
    public void testOutInToSelf() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("aOutB", b1);
        this.sqlgGraph.tx().commit();
        assertEquals(1, a1.out().in().count().next().intValue());
    }

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

        assertEquals(9, a1.out().in().out().count().next().intValue());
    }
}
