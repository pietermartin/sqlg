package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertArrayEquals;

/**
 * Date: 2016/05/22
 * Time: 9:09 AM
 */
public class TestNormalBatchPrimitiveArrays extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testBatchArrayString() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new String[]{"a", "b"});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        String[] array = god.value("array");
        assertArrayEquals(array, new String[]{"a", "b"});
    }

    @Test
    public void testBatchArrayStringEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new String[]{"a", "b"});
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        String[] array = e1.value("array");
        assertArrayEquals(array, new String[]{"a", "b"});
    }

    @Test
    public void testBatchArrayshort() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new short[]{2, 1});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        short[] array = god.value("array");
        assertArrayEquals(new short[]{2, 1}, array);
    }

    @Test
    public void testBatchArrayShort() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Short[]{2, 1});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Short[] array = god.value("array");
        assertArrayEquals(new Short[]{2, 1}, array);
    }

    @Test
    public void testBatchArrayshortEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new short[]{2, 1});
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        short[] array = e1.value("array");
        assertArrayEquals(new short[]{2, 1}, array);
    }

    @Test
    public void testBatchArrayShortEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new Short[]{2, 1});
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        Short[] array = e1.value("array");
        assertArrayEquals(new Short[]{2, 1}, array);
    }

    @Test
    public void testBatchArrayInt() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new int[]{2, 1});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        int[] array = god.value("array");
        assertArrayEquals(new int[]{2, 1}, array);
    }

    @Test
    public void testBatchArrayInteger() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Integer[]{2, 1});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Integer[] array = god.value("array");
        assertArrayEquals(new Integer[]{2, 1}, array);
    }

    @Test
    public void testBatchArrayIntEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new int[]{2, 1});
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        int[] array = e1.value("array");
        assertArrayEquals(new int[]{2, 1}, array);
    }

    @Test
    public void testBatchArrayIntegerEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new Integer[]{2, 1});
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        Integer[] array = e1.value("array");
        assertArrayEquals(new Integer[]{2, 1}, array);
    }

    @Test
    public void testBatchArrayboolean() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new boolean[]{true, false});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        boolean[] array = god.value("array");
        assertArrayEquals(new boolean[]{true, false}, array);
    }

    @Test
    public void testBatchArrayBoolean() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Boolean[]{true, false});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Boolean[] array = god.value("array");
        assertArrayEquals(new Boolean[]{true, false}, array);
    }

    @Test
    public void testBatchArraybooleanEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new boolean[]{true, false});
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        boolean[] array = e1.value("array");
        assertArrayEquals(new boolean[]{true, false}, array);
    }

    @Test
    public void testBatchArrayBooleanEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new Boolean[]{true, false});
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        Boolean[] array = e1.value("array");
        assertArrayEquals(new Boolean[]{true, false}, array);
    }

    @Test
    public void testBatchArraybyte() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new byte[]{1, 3});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        byte[] array = god.value("array");
        assertArrayEquals(new byte[]{1, 3}, array);
    }

    @Test
    public void testBatchArrayByte() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Byte[] bytes = {1, 3};
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", bytes);
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Byte[] array = god.value("array");
        assertArrayEquals(new Byte[]{1, 3}, array);
    }

    @Test
    public void testBatchArraybyteEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new byte[]{1, 3});
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        byte[] array = e1.value("array");
        assertArrayEquals(new byte[]{1, 3}, array);
    }

    @Test
    public void testBatchArrayByteEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Byte[] bytes = {1, 3};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", bytes);
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        Byte[] array = e1.value("array");
        assertArrayEquals(new Byte[]{1, 3}, array);
    }

    @Test
    public void testBatchArraylong() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new long[]{1L, 3L});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        long[] array = god.value("array");
        assertArrayEquals(new long[]{1L, 3L}, array);
    }

    @Test
    public void testBatchArrayLong() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Long[] longs = {1L, 3L};
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", longs);
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Long[] array = god.value("array");
        assertArrayEquals(new Long[]{1L, 3L}, array);
    }

    @Test
    public void testBatchArraylongEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new long[]{1L, 3L});
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        long[] array = e1.value("array");
        assertArrayEquals(new long[]{1L, 3L}, array);
    }

    @Test
    public void testBatchArrayLongEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Long[] longs = {1L, 3L};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", longs);
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        Long[] array = e1.value("array");
        assertArrayEquals(new Long[]{1L, 3L}, array);
    }

    @Test
    public void testBatchArrayfloat() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new float[]{1.1f, 3.3f});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        float[] array = god.value("array");
        assertArrayEquals(new float[]{1.1f, 3.3f}, array, 0f);
    }

    @Test
    public void testBatchArrayFloat() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Float[]{1.1f, 2.2f});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Float[] array = god.value("array");
        assertArrayEquals(new Float[]{1.1f, 2.2f}, array);
    }

    @Test
    public void testBatchArrayfloatEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new float[]{1.1f, 3.3f});
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        float[] array = e1.value("array");
        assertArrayEquals(new float[]{1.1f, 3.3f}, array, 0f);
    }

    @Test
    public void testBatchArrayFloatEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new Float[]{1.1f, 2.2f});
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        Float[] array = e1.value("array");
        assertArrayEquals(new Float[]{1.1f, 2.2f}, array);
    }

    @Test
    public void testBatchArraydouble() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new double[]{1.1d, 3.3d});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        double[] array = god.value("array");
        assertArrayEquals(new double[]{1.1d, 3.3d}, array, 0d);
    }

    @Test
    public void testBatchArrayDouble() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Double[]{1.1d, 2.2d});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Double[] array = god.value("array");
        assertArrayEquals(new Double[]{1.1d, 2.2d}, array);
    }

    @Test
    public void testBatchArraydoubleEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new double[]{1.1d, 3.3d});
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        double[] array = e1.value("array");
        assertArrayEquals(new double[]{1.1d, 3.3d}, array, 0d);
    }

    @Test
    public void testBatchArrayDoubleEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new Double[]{1.1d, 2.2d});
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        Double[] array = e1.value("array");
        assertArrayEquals(new Double[]{1.1d, 2.2d}, array);
    }

}
