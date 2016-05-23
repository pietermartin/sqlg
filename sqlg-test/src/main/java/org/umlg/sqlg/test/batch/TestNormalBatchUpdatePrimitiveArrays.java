package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertArrayEquals;

/**
 * Date: 2016/05/22
 * Time: 9:16 AM
 */
public class TestNormalBatchUpdatePrimitiveArrays extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testBatchUpdateArrayString() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new String[]{"a", "b"});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        god.property("array", new String[]{"c", "d"});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        String[] array = god.value("array");
        assertArrayEquals(array, new String[]{"c", "d"});
    }

    @Test
    public void testBatchUpdateArrayshort() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new short[]{2, 1});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        god.property("array", new short[]{4, 5});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        short[] array = god.value("array");
        assertArrayEquals(new short[]{4, 5}, array);
    }

    @Test
    public void testBatchUpdateArrayShort() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Short[]{2, 1});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        god.property("array", new Short[]{4, 5});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Short[] array = god.value("array");
        assertArrayEquals(new Short[]{4,5}, array);
    }

    @Test
    public void testBatchUpdateArrayInt() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new int[]{2, 1});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        god.property("array", new int[]{1, 2});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        int[] array = god.value("array");
        assertArrayEquals(new int[]{1, 2}, array);
    }

    @Test
    public void testBatchUpdateArrayInteger() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Integer[]{2, 1});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        god.property("array", new Integer[]{1, 2});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Integer[] array = god.value("array");
        assertArrayEquals(new Integer[]{1, 2}, array);
    }

    @Test
    public void testBatchUpdateArrayboolean() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new boolean[]{true, false});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        god.property("array", new boolean[]{false, true});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        boolean[] array = god.value("array");
        assertArrayEquals(new boolean[]{false, true}, array);
    }

    @Test
    public void testBatchUpdateArrayBoolean() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Boolean[]{true, false});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        god.property("array", new Boolean[]{false, true});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Boolean[] array = god.value("array");
        assertArrayEquals(new Boolean[]{false, true}, array);
    }

    @Test
    public void testBatchUpdateArraybyte() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new byte[]{1, 3});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        god.property("array", new byte[]{5, 6});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        byte[] array = god.value("array");
        assertArrayEquals(new byte[]{5, 6}, array);
    }

    @Test
    public void testBatchUpdateArrayByte() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Byte[] bytes = {1, 3};
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", bytes);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        god.property("array", new Byte[]{6, 7});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Byte[] array = god.value("array");
        assertArrayEquals(new Byte[]{6, 7}, array);
    }

    @Test
    public void testBatchUpdateArraylong() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new long[]{1L, 3L});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        god.property("array", new long[]{5L, 6L});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        long[] array = god.value("array");
        assertArrayEquals(new long[]{5L, 6L}, array);
    }

    @Test
    public void testBatchUpdateArrayLong() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Long[] longs = {1L, 3L};
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", longs);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        god.property("array", new Long[]{6L, 7L});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Long[] array = god.value("array");
        assertArrayEquals(new Long[]{6L, 7L}, array);
    }

    @Test
    public void testBatchUpdateArrayfloat() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new float[]{1.1f, 3.3f});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        god.property("array", new float[]{5.5f, 6.6f});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        float[] array = god.value("array");
        assertArrayEquals(new float[]{5.5f, 6.6f}, array, 0f);
    }

    @Test
    public void testBatchUpdateArrayFloat() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Float[]{1.1f, 2.2f});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        god.property("array", new Float[]{6.6f, 7.7f});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Float[] array = god.value("array");
        assertArrayEquals(new Float[]{6.6f, 7.7f}, array);
    }

    @Test
    public void testBatchUpdateArraydouble() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new double[]{1.1d, 3.3d});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        god.property("array", new double[]{5.5d, 6.6d});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        double[] array = god.value("array");
        assertArrayEquals(new double[]{5.5d, 6.6d}, array, 0d);
    }

    @Test
    public void testBatchUpdateArrayDouble() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Double[]{1.1d, 2.2d});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        god.property("array", new Double[]{6.6d, 7.7d});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Double[] array = god.value("array");
        assertArrayEquals(new Double[]{6.6d, 7.7d}, array);
    }
}
