package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.Arrays;

/**
 * Date: 2014/07/19
 * Time: 2:16 PM
 */
public class TestArrayProperties extends BaseTest {

    @Test
    public void testBooleanArrayProperties() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsBooleanArrayValues());
        this.sqlG.addVertex(Element.LABEL, "Person", "age", new boolean[]{true, false, true, false, true});
        this.sqlG.tx().commit();
        Vertex v = this.sqlG.V().next();
        Assert.assertTrue(Arrays.equals(new boolean[]{true, false, true, false, true}, (boolean[]) v.property("age").value()));
    }

    @Test
    public void testShortArrayProperties() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsShortArrayValues());
        this.sqlG.addVertex(Element.LABEL, "Person", "age", new short[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Vertex v = this.sqlG.V().next();
        Assert.assertTrue(Arrays.equals(new short[]{1, 2, 3, 4, 5}, (short[]) v.property("age").value()));
    }

    @Test
    public void testIntArrayProperties() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsIntegerArrayValues());
        this.sqlG.addVertex(Element.LABEL, "Person", "age", new int[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Vertex v = this.sqlG.V().next();
        Assert.assertTrue(Arrays.equals(new int[]{1, 2, 3, 4, 5}, (int[]) v.property("age").value()));
    }

    @Test
    public void testLongArrayProperties() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsLongArrayValues());
        this.sqlG.addVertex(Element.LABEL, "Person", "age", new long[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Vertex v = this.sqlG.V().next();
        Assert.assertTrue(Arrays.equals(new long[]{1, 2, 3, 4, 5}, (long[]) v.property("age").value()));
    }

    @Test
    public void testFloatArrayProperties() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsFloatArrayValues());
        this.sqlG.addVertex(Element.LABEL, "Person", "age", new float[]{1f, 2f, 3f, 4f, 5f});
        this.sqlG.tx().commit();
        Vertex v = this.sqlG.V().next();
        Assert.assertTrue(Arrays.equals(new float[]{1, 2, 3, 4, 5}, (float[]) v.property("age").value()));
    }

    @Test
    public void testDoubleArrayProperties() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsDoubleArrayValues());
        this.sqlG.addVertex(Element.LABEL, "Person", "age", new double[]{1d, 2d, 3d, 4d, 5d});
        this.sqlG.tx().commit();
        Vertex v = this.sqlG.V().next();
        Assert.assertTrue(Arrays.equals(new double[]{1, 2, 3, 4, 5}, (double[]) v.property("age").value()));
    }

    @Test
    public void testStringArrayProperties() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsStringArrayValues());
        this.sqlG.addVertex(Element.LABEL, "Person", "age", new String[]{"a", "b", "c", "d", "e"});
        this.sqlG.tx().commit();
        Vertex v = this.sqlG.V().next();
        Assert.assertTrue(Arrays.equals(new String[]{"a", "b", "c", "d", "e"}, (String[]) v.property("age").value()));
    }

    @Test
    public void testBooleanArrayEdgeProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsBooleanArrayValues());
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2, "weight", new boolean[]{true, false, true, false, true});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new boolean[]{true, false, true, false, true}, (boolean[]) e.property("weight").value()));
    }

    @Test
    public void testShortArrayEdgeProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsShortArrayValues());
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2, "weight", new short[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new short[]{1, 2, 3, 4, 5}, (short[]) e.property("weight").value()));
    }

    @Test
    public void testIntArrayEdgeProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsIntegerArrayValues());
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2, "weight", new int[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new int[]{1, 2, 3, 4, 5}, (int[]) e.property("weight").value()));
    }

    @Test
    public void testLongArrayEdgeProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsLongArrayValues());
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2, "weight", new long[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new long[]{1, 2, 3, 4, 5}, (long[]) e.property("weight").value()));
    }

    @Test
    public void testFloatArrayEdgeProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsFloatArrayValues());
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2, "weight", new float[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new float[]{1, 2, 3, 4, 5}, (float[]) e.property("weight").value()));
    }

    @Test
    public void testDoubleArrayEdgeProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsDoubleArrayValues());
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2, "weight", new double[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new double[]{1, 2, 3, 4, 5}, (double[]) e.property("weight").value()));
    }

    @Test
    public void testStringArrayEdgeProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsStringArrayValues());
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2, "weight", new String[]{"a", "b", "c", "d", "e"});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new String[]{"a", "b", "c", "d", "e"}, (String[]) e.property("weight").value()));
    }

    @Test
    public void testAddBooleanArrayProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsBooleanArrayValues());
        Vertex v = this.sqlG.addVertex(Element.LABEL, "Person");
        v.property("age", new boolean[]{true, false, true, false, true});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new boolean[]{true, false, true, false, true}, (boolean[]) v.property("age").value()));
    }

    @Test
    public void testAddShortArrayProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsShortArrayValues());
        Vertex v = this.sqlG.addVertex(Element.LABEL, "Person");
        v.property("age", new short[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new short[]{1, 2, 3, 4, 5}, (short[]) v.property("age").value()));
    }

    @Test
    public void testAddIntArrayProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsIntegerArrayValues());
        Vertex v = this.sqlG.addVertex(Element.LABEL, "Person");
        v.property("age", new int[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new int[]{1, 2, 3, 4, 5}, (int[]) v.property("age").value()));
    }

    @Test
    public void testAddLongArrayProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsLongArrayValues());
        Vertex v = this.sqlG.addVertex(Element.LABEL, "Person");
        v.property("age", new long[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new long[]{1, 2, 3, 4, 5}, (long[]) v.property("age").value()));
    }

    @Test
    public void testAddDoubleArrayProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsDoubleArrayValues());
        Vertex v = this.sqlG.addVertex(Element.LABEL, "Person");
        v.property("age", new double[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new double[]{1, 2, 3, 4, 5}, (double[]) v.property("age").value()));
    }

    @Test
    public void testAddStringArrayProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsStringArrayValues());
        Vertex v = this.sqlG.addVertex(Element.LABEL, "Person");
        v.property("age", new String[]{"a", "b", "c", "d", "e"});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new String[]{"a", "b", "c", "d", "e"}, (String[]) v.property("age").value()));
    }

    @Test
    public void testAddEdgeBooleanArrayProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsBooleanArrayValues());
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2);
        e.property("age", new boolean[]{true, false, true, false, true});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new boolean[]{true, false, true, false, true}, (boolean[]) e.property("age").value()));
    }

    @Test
    public void testAddEdgeShortArrayProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsShortArrayValues());
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2);
        e.property("age", new short[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new short[]{1, 2, 3, 4, 5}, (short[]) e.property("age").value()));
    }

    @Test
    public void testAddEdgeIntArrayProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsIntegerArrayValues());
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2);
        e.property("age", new int[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new int[]{1, 2, 3, 4, 5}, (int[]) e.property("age").value()));
    }

    @Test
    public void testAddEdgeLongArrayProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsLongArrayValues());
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2);
        e.property("age", new long[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new long[]{1, 2, 3, 4, 5}, (long[]) e.property("age").value()));
    }

    @Test
    public void testAddEdgeFloatArrayProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsFloatArrayValues());
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2);
        e.property("age", new float[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new float[]{1, 2, 3, 4, 5}, (float[]) e.property("age").value()));
    }

    @Test
    public void testAddEdgeDoubleArrayProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsDoubleArrayValues());
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2);
        e.property("age", new double[]{1, 2, 3, 4, 5});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new double[]{1, 2, 3, 4, 5}, (double[]) e.property("age").value()));
    }

    @Test
    public void testAddEdgeStringArrayProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsStringArrayValues());
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2);
        e.property("age", new String[]{"a", "b", "c", "d", "e"});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new String[]{"a", "b", "c", "d", "e"}, (String[]) e.property("age").value()));
    }

    @Test
    public void testUpdateEdgeStringArrayProperty() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsStringArrayValues());
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        Edge e = v1.addEdge("friend", v2);
        e.property("age", new String[]{"a", "b", "c", "d", "e"});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new String[]{"a", "b", "c", "d", "e"}, (String[]) e.property("age").value()));
        e.property("age", new String[]{"e", "d", "c", "b", "a"});
        this.sqlG.tx().commit();
        Assert.assertTrue(Arrays.equals(new String[]{"e", "d", "c", "b", "a"}, (String[]) e.property("age").value()));
    }
}
