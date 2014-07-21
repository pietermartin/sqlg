package org.umlg.sqlgraph.test;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Date: 2014/07/19
 * Time: 2:16 PM
 */
public class TestArrayProperties extends BaseTest {

    @Test
    public void testBooleanArrayProperties() {
        this.sqlGraph.addVertex(Element.LABEL, "Person", "age", new boolean[]{true, false, true, false, true});
        this.sqlGraph.tx().commit();
        Vertex v = this.sqlGraph.V().next();
        Assert.assertTrue(Arrays.equals(new boolean[]{true, false, true, false, true}, (boolean[]) v.property("age").value()));
    }

    @Test
    public void testShortArrayProperties() {
        this.sqlGraph.addVertex(Element.LABEL, "Person", "age", new short[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Vertex v = this.sqlGraph.V().next();
        Assert.assertTrue(Arrays.equals(new short[]{1, 2, 3, 4, 5}, (short[]) v.property("age").value()));
    }

    @Test
    public void testIntArrayProperties() {
        this.sqlGraph.addVertex(Element.LABEL, "Person", "age", new int[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Vertex v = this.sqlGraph.V().next();
        Assert.assertTrue(Arrays.equals(new int[]{1, 2, 3, 4, 5}, (int[]) v.property("age").value()));
    }

    @Test
    public void testLongArrayProperties() {
        this.sqlGraph.addVertex(Element.LABEL, "Person", "age", new long[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Vertex v = this.sqlGraph.V().next();
        Assert.assertTrue(Arrays.equals(new long[]{1, 2, 3, 4, 5}, (long[]) v.property("age").value()));
    }

    @Test
    public void testFloatArrayProperties() {
        this.sqlGraph.addVertex(Element.LABEL, "Person", "age", new float[]{1f, 2f, 3f, 4f, 5f});
        this.sqlGraph.tx().commit();
        Vertex v = this.sqlGraph.V().next();
        Assert.assertTrue(Arrays.equals(new float[]{1, 2, 3, 4, 5}, (float[]) v.property("age").value()));
    }

    @Test
    public void testDoubleArrayProperties() {
        this.sqlGraph.addVertex(Element.LABEL, "Person", "age", new double[]{1d, 2d, 3d, 4d, 5d});
        this.sqlGraph.tx().commit();
        Vertex v = this.sqlGraph.V().next();
        Assert.assertTrue(Arrays.equals(new double[]{1, 2, 3, 4, 5}, (double[]) v.property("age").value()));
    }

    @Test
    public void testStringArrayProperties() {
        this.sqlGraph.addVertex(Element.LABEL, "Person", "age", new String[]{"a", "b", "c", "d", "e"});
        this.sqlGraph.tx().commit();
        Vertex v = this.sqlGraph.V().next();
        Assert.assertTrue(Arrays.equals(new String[]{"a", "b", "c", "d", "e"}, (String[]) v.property("age").value()));
    }

    @Test
    public void testBooleanArrayEdgeProperty() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2, "weight", new boolean[]{true, false, true, false, true});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new boolean[]{true, false, true, false, true}, (boolean[]) e.property("weight").value()));
    }

    @Test
    public void testShortArrayEdgeProperty() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2, "weight", new short[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new short[]{1, 2, 3, 4, 5}, (short[]) e.property("weight").value()));
    }

    @Test
    public void testIntArrayEdgeProperty() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2, "weight", new int[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new int[]{1, 2, 3, 4, 5}, (int[]) e.property("weight").value()));
    }

    @Test
    public void testLongArrayEdgeProperty() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2, "weight", new long[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new long[]{1, 2, 3, 4, 5}, (long[]) e.property("weight").value()));
    }

    @Test
    public void testFloatArrayEdgeProperty() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2, "weight", new float[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new float[]{1, 2, 3, 4, 5}, (float[]) e.property("weight").value()));
    }

    @Test
    public void testDoubleArrayEdgeProperty() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2, "weight", new double[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new double[]{1, 2, 3, 4, 5}, (double[]) e.property("weight").value()));
    }

    @Test
    public void testStringArrayEdgeProperty() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2, "weight", new String[]{"a", "b", "c", "d", "e"});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new String[]{"a", "b", "c", "d", "e"}, (String[]) e.property("weight").value()));
    }

    @Test
    public void testAddBooleanArrayProperty() {
        Vertex v = this.sqlGraph.addVertex(Element.LABEL, "Person");
        v.property("age", new boolean[]{true, false, true, false, true});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new boolean[]{true, false, true, false, true}, (boolean[]) v.property("age").value()));
    }

    @Test
    public void testAddShortArrayProperty() {
        Vertex v = this.sqlGraph.addVertex(Element.LABEL, "Person");
        v.property("age", new short[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new short[]{1, 2, 3, 4, 5}, (short[]) v.property("age").value()));
    }

    @Test
    public void testAddIntArrayProperty() {
        Vertex v = this.sqlGraph.addVertex(Element.LABEL, "Person");
        v.property("age", new int[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new int[]{1, 2, 3, 4, 5}, (int[]) v.property("age").value()));
    }

    @Test
    public void testAddLongArrayProperty() {
        Vertex v = this.sqlGraph.addVertex(Element.LABEL, "Person");
        v.property("age", new long[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new long[]{1, 2, 3, 4, 5}, (long[]) v.property("age").value()));
    }

    @Test
    public void testAddDoubleArrayProperty() {
        Vertex v = this.sqlGraph.addVertex(Element.LABEL, "Person");
        v.property("age", new double[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new double[]{1, 2, 3, 4, 5}, (double[]) v.property("age").value()));
    }

    @Test
    public void testAddStringArrayProperty() {
        Vertex v = this.sqlGraph.addVertex(Element.LABEL, "Person");
        v.property("age", new String[]{"a", "b", "c", "d", "e"});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new String[]{"a", "b", "c", "d", "e"}, (String[]) v.property("age").value()));
    }

    @Test
    public void testAddEdgeBooleanArrayProperty() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2);
        e.property("age", new boolean[]{true, false, true, false, true});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new boolean[]{true, false, true, false, true}, (boolean[]) e.property("age").value()));
    }

    @Test
    public void testAddEdgeShortArrayProperty() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2);
        e.property("age", new short[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new short[]{1, 2, 3, 4, 5}, (short[]) e.property("age").value()));
    }

    @Test
    public void testAddEdgeIntArrayProperty() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2);
        e.property("age", new int[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new int[]{1, 2, 3, 4, 5}, (int[]) e.property("age").value()));
    }

    @Test
    public void testAddEdgeLongArrayProperty() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2);
        e.property("age", new long[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new long[]{1, 2, 3, 4, 5}, (long[]) e.property("age").value()));
    }

    @Test
    public void testAddEdgeFloatArrayProperty() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2);
        e.property("age", new float[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new float[]{1, 2, 3, 4, 5}, (float[]) e.property("age").value()));
    }

    @Test
    public void testAddEdgeDoubleArrayProperty() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2);
        e.property("age", new double[]{1, 2, 3, 4, 5});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new double[]{1, 2, 3, 4, 5}, (double[]) e.property("age").value()));
    }

    @Test
    public void testAddEdgeStringArrayProperty() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2);
        e.property("age", new String[]{"a", "b", "c", "d", "e"});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new String[]{"a", "b", "c", "d", "e"}, (String[]) e.property("age").value()));
    }

    @Test
    public void testUpdateEdgeStringArrayProperty() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2);
        e.property("age", new String[]{"a", "b", "c", "d", "e"});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new String[]{"a", "b", "c", "d", "e"}, (String[]) e.property("age").value()));
        e.property("age", new String[]{"e", "d", "c", "b", "a"});
        this.sqlGraph.tx().commit();
        Assert.assertTrue(Arrays.equals(new String[]{"e", "d", "c", "b", "a"}, (String[]) e.property("age").value()));
    }
}
