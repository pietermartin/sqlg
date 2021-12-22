package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

/**
 * Date: 2014/07/19
 * Time: 2:16 PM
 */
public class TestArrayProperties extends BaseTest {

    @Test
    public void testBytePrimitiveArrayProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsByteArrayValues());
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "Person", "age", new byte[]{1, 2, 3, 4, 5});
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "Person", "age", new byte[]{1, 2, 3, 4, 5});
        vertex1.addEdge("test", vertex2, "age", new byte[]{1, 2, 3, 4, 5});
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.traversal().V().next();
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, (byte[]) v.property("age").value());
        Edge e = this.sqlgGraph.traversal().E().next();
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, (byte[]) e.property("age").value());
    }

    @Test
    public void testByteArrayProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsByteArrayValues());
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "Person", "age", new Byte[]{1, 2, 3, 4, 5});
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "Person", "age", new Byte[]{1, 2, 3, 4, 5});
        vertex1.addEdge("test", vertex2, "age", new Byte[]{1, 2, 3, 4, 5});
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.traversal().V().next();
        assertArrayEquals(new Byte[]{1, 2, 3, 4, 5}, (Byte[]) v.property("age").value());
        Edge e = this.sqlgGraph.traversal().E().next();
        assertArrayEquals(new Byte[]{1, 2, 3, 4, 5}, (Byte[]) e.property("age").value());
    }

    @Test
    public void testBooleanPrimitiveArrayProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBooleanArrayValues());
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "Person", "age", new boolean[]{true, false, true, false, true});
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "Person", "age", new boolean[]{true, false, true, false, true});
        vertex1.addEdge("test", vertex2, "age", new boolean[]{true, false, true, false, true});
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.traversal().V().next();
        assertArrayEquals(new boolean[]{true, false, true, false, true}, (boolean[]) v.property("age").value());
        Edge e = this.sqlgGraph.traversal().E().next();
        assertArrayEquals(new boolean[]{true, false, true, false, true}, (boolean[]) e.property("age").value());
    }

    @Test
    public void testBooleanArrayProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBooleanArrayValues());
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "Person", "age", new Boolean[]{true, false, true, false, true});
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "Person", "age", new Boolean[]{true, false, true, false, true});
        vertex1.addEdge("test", vertex2, "age", new Boolean[]{true, false, true, false, true});
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.traversal().V().next();
        assertArrayEquals(new Boolean[]{true, false, true, false, true}, (Boolean[]) v.property("age").value());
        Edge e = this.sqlgGraph.traversal().E().next();
        assertArrayEquals(new Boolean[]{true, false, true, false, true}, (Boolean[]) e.property("age").value());
    }

    @Test
    public void testShortPrimitiveArrayProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsShortArrayValues());
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "Person", "age", new short[]{1, 2, 3, 4, 5});
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "Person", "age", new short[]{1, 2, 3, 4, 5});
        vertex1.addEdge("test", vertex2, "age", new short[]{1, 2, 3, 4, 5});
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.traversal().V().next();
        assertArrayEquals(new short[]{1, 2, 3, 4, 5}, (short[]) v.property("age").value());
        Edge e = this.sqlgGraph.traversal().E().next();
        assertArrayEquals(new short[]{1, 2, 3, 4, 5}, (short[]) e.property("age").value());
    }

    @Test
    public void testShortArrayProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsShortArrayValues());
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "Person", "age", new Short[]{1, 2, 3, 4, 5});
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "Person", "age", new Short[]{1, 2, 3, 4, 5});
        vertex1.addEdge("test", vertex2, "age", new Short[]{1, 2, 3, 4, 5});
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.traversal().V().next();
        assertArrayEquals(new Short[]{1, 2, 3, 4, 5}, (Short[]) v.property("age").value());
        Edge e = this.sqlgGraph.traversal().E().next();
        assertArrayEquals(new Short[]{1, 2, 3, 4, 5}, (Short[]) e.property("age").value());
    }

    @Test
    public void testIntPrimitiveArrayProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "Person", "age", new int[]{1, 2, 3, 4, 5});
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "Person", "age", new int[]{1, 2, 3, 4, 5});
        vertex1.addEdge("test", vertex2, "age", new int[]{1, 2, 3, 4, 5});
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.traversal().V().next();
        assertArrayEquals(new int[]{1, 2, 3, 4, 5}, (int[]) v.property("age").value());
        Edge e = this.sqlgGraph.traversal().E().next();
        assertArrayEquals(new int[]{1, 2, 3, 4, 5}, (int[]) e.property("age").value());
    }

    @Test
    public void testIntegerArrayProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "Person", "age", new Integer[]{1, 2, 3, 4, 5});
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "Person", "age", new Integer[]{1, 2, 3, 4, 5});
        vertex1.addEdge("test", vertex2, "age", new Integer[]{1, 2, 3, 4, 5});
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.traversal().V().next();
        assertArrayEquals(new Integer[]{1, 2, 3, 4, 5}, (Integer[]) v.property("age").value());
        Edge e = this.sqlgGraph.traversal().E().next();
        assertArrayEquals(new Integer[]{1, 2, 3, 4, 5}, (Integer[]) e.property("age").value());
    }

    @Test
    public void testLongPrimitiveArrayProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLongArrayValues());
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "Person", "age", new long[]{1, 2, 3, 4, 5});
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "Person", "age", new long[]{1, 2, 3, 4, 5});
        vertex1.addEdge("test", vertex2, "age", new long[]{1, 2, 3, 4, 5});
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.traversal().V().next();
        assertArrayEquals(new long[]{1, 2, 3, 4, 5}, (long[]) v.property("age").value());
        Edge e = this.sqlgGraph.traversal().E().next();
        assertArrayEquals(new long[]{1, 2, 3, 4, 5}, (long[]) e.property("age").value());
    }

    @Test
    public void testLongArrayProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLongArrayValues());
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "Person", "age", new Long[]{1L, 2L, 3L, 4L, 5L});
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "Person", "age", new Long[]{1L, 2L, 3L, 4L, 5L});
        vertex1.addEdge("test", vertex2, "age", new Long[]{1L, 2L, 3L, 4L, 5L});
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.traversal().V().next();
        assertArrayEquals(new Long[]{1L, 2L, 3L, 4L, 5L}, (Long[]) v.property("age").value());
        Edge e = this.sqlgGraph.traversal().E().next();
        assertArrayEquals(new Long[]{1L, 2L, 3L, 4L, 5L}, (Long[]) e.property("age").value());
    }

    @Test
    public void testFloatArrayProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatArrayValues());
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "Person", "age", new Float[]{1F, 2F, 3F, 4F, 5F});
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "Person", "age", new Float[]{1F, 2F, 3F, 4F, 5F});
        vertex1.addEdge("test", vertex2, "age", new Float[]{1F, 2F, 3F, 4F, 5F});
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.traversal().V().next();
        assertArrayEquals(new Float[]{1F, 2F, 3F, 4F, 5F}, (Float[]) v.property("age").value());
        Edge e = this.sqlgGraph.traversal().E().next();
        assertArrayEquals(new Float[]{1F, 2F, 3F, 4F, 5F}, (Float[]) e.property("age").value());
    }

    @SuppressWarnings("SimplifiableAssertion")
    @Test
    public void testFloatPrimitiveArrayProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatArrayValues());
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "Person", "age", new float[]{1F, 2F, 3F, 4F, 5f});
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "Person", "age", new float[]{1F, 2F, 3F, 4F, 5f});
        vertex1.addEdge("test", vertex2, "age", new float[]{1F, 2F, 3F, 4F, 5f});
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.traversal().V().next();
        Assert.assertTrue(Arrays.equals(new float[]{1F, 2F, 3F, 4F, 5F}, (float[]) v.property("age").value()));
        Edge e = this.sqlgGraph.traversal().E().next();
        Assert.assertTrue(Arrays.equals(new float[]{1F, 2F, 3F, 4F, 5F}, (float[]) e.property("age").value()));
    }

    @Test
    public void testDoubleArrayProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDoubleArrayValues());
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "Person", "age", new Double[]{1D, 2D, 3D, 4D, 5D});
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "Person", "age", new Double[]{1D, 2D, 3D, 4D, 5D});
        vertex1.addEdge("test", vertex2, "age", new Double[]{1D, 2D, 3D, 4D, 5D});
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.traversal().V().next();
        assertArrayEquals(new Double[]{1D, 2D, 3D, 4D, 5D}, (Double[]) v.property("age").value());
        Edge e = this.sqlgGraph.traversal().E().next();
        assertArrayEquals(new Double[]{1D, 2D, 3D, 4D, 5D}, (Double[]) e.property("age").value());
    }

    @Test
    public void testDoublePrimitiveArrayProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDoubleArrayValues());
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "Person", "age", new double[]{1d, 2d, 3d, 4d, 5d});
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "Person", "age", new double[]{1d, 2d, 3d, 4d, 5d});
        vertex1.addEdge("test", vertex2, "age", new double[]{1d, 2d, 3d, 4d, 5d});
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.traversal().V().next();
        assertArrayEquals(new double[]{1, 2, 3, 4, 5}, (double[]) v.property("age").value(), 0.0);
        Edge e = this.sqlgGraph.traversal().E().next();
        assertArrayEquals(new double[]{1, 2, 3, 4, 5}, (double[]) e.property("age").value(), 0.0);
    }

    @Test
    public void testStringArrayProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStringArrayValues());
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "Person", "age", new String[]{"a", "b", "c", "d", "e"});
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "Person", "age", new String[]{"a", "b", "c", "d", "e"});
        vertex1.addEdge("test", vertex2, "age", new String[]{"a", "b", "c", "d", "e"});
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.traversal().V().next();
        assertArrayEquals(new String[]{"a", "b", "c", "d", "e"}, (String[]) v.property("age").value());
        Edge e = this.sqlgGraph.traversal().E().next();
        assertArrayEquals(new String[]{"a", "b", "c", "d", "e"}, (String[]) e.property("age").value());
    }

}
