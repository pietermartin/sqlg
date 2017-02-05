package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;

/**
 * Date: 2016/05/22
 * Time: 9:09 AM
 */
public class TestBatchNormalPrimitiveArrays extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        BaseTest.beforeClass();
        if (configuration.getString("jdbc.url").contains("postgresql")) {
            configuration.addProperty("distributed", true);
        }
    }

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testBatchArrayString() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new String[]{"a", "b"});
        this.sqlgGraph.tx().commit();
        testBatchArrayString_assert(this.sqlgGraph, god);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayString_assert(this.sqlgGraph1, god);
        }
    }

    private void testBatchArrayString_assert(SqlgGraph sqlgGraph, Vertex god) {
        god = sqlgGraph.traversal().V(god.id()).next();
        String[] array = god.value("array");
        assertArrayEquals(array, new String[]{"a", "b"});
    }

    @Test
    public void testBatchArrayStringEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new String[]{"a", "b"});
        this.sqlgGraph.tx().commit();
        testBatchArrayStringEdge_assert(this.sqlgGraph, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayStringEdge_assert(this.sqlgGraph1, e1);
        }
    }

    private void testBatchArrayStringEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
        e1 = sqlgGraph.traversal().E(e1.id()).next();
        String[] array = e1.value("array");
        assertArrayEquals(array, new String[]{"a", "b"});
    }

    @Test
    public void testBatchArrayshort() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new short[]{2, 1});
        this.sqlgGraph.tx().commit();
        testBatchArrayshort_assert(this.sqlgGraph, god);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayshort_assert(this.sqlgGraph1, god);
        }
    }

    private void testBatchArrayshort_assert(SqlgGraph sqlgGraph, Vertex god) {
        god = sqlgGraph.traversal().V(god.id()).next();
        short[] array = god.value("array");
        assertArrayEquals(new short[]{2, 1}, array);
    }

    @Test
    public void testBatchArrayShort() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Short[]{2, 1});
        this.sqlgGraph.tx().commit();
        testBatchArrayShort_assert(this.sqlgGraph, god);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayShort_assert(this.sqlgGraph1, god);
        }
    }

    private void testBatchArrayShort_assert(SqlgGraph sqlgGraph, Vertex god) {
        god = sqlgGraph.traversal().V(god.id()).next();
        Short[] array = god.value("array");
        assertArrayEquals(new Short[]{2, 1}, array);
    }

    @Test
    public void testBatchArrayshortEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new short[]{2, 1});
        this.sqlgGraph.tx().commit();
        testBatchArrayshortEdge_assert(this.sqlgGraph, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayshortEdge_assert(this.sqlgGraph1, e1);
        }
    }

    private void testBatchArrayshortEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
        e1 = sqlgGraph.traversal().E(e1.id()).next();
        short[] array = e1.value("array");
        assertArrayEquals(new short[]{2, 1}, array);
    }

    @Test
    public void testBatchArrayShortEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new Short[]{2, 1});
        this.sqlgGraph.tx().commit();
        testBatchArrayShortEdge_assert(this.sqlgGraph, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayShortEdge_assert(this.sqlgGraph1, e1);
        }
    }

    private void testBatchArrayShortEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
        e1 = sqlgGraph.traversal().E(e1.id()).next();
        Short[] array = e1.value("array");
        assertArrayEquals(new Short[]{2, 1}, array);
    }

    @Test
    public void testBatchArrayInt() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new int[]{2, 1});
        this.sqlgGraph.tx().commit();
        testBatchArrayInt_assert(this.sqlgGraph, god);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayInt_assert(this.sqlgGraph1, god);
        }
    }

    private void testBatchArrayInt_assert(SqlgGraph sqlgGraph, Vertex god) {
        god = sqlgGraph.traversal().V(god.id()).next();
        int[] array = god.value("array");
        assertArrayEquals(new int[]{2, 1}, array);
    }

    @Test
    public void testBatchArrayInteger() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Integer[]{2, 1});
        this.sqlgGraph.tx().commit();
        testBatchArrayInteger_assert(this.sqlgGraph, god);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayInteger_assert(this.sqlgGraph1, god);
        }
    }

    private void testBatchArrayInteger_assert(SqlgGraph sqlgGraph, Vertex god) {
        god = sqlgGraph.traversal().V(god.id()).next();
        Integer[] array = god.value("array");
        assertArrayEquals(new Integer[]{2, 1}, array);
    }

    @Test
    public void testBatchArrayIntEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new int[]{2, 1});
        this.sqlgGraph.tx().commit();
        testBatchArrayIntEdge_assert(this.sqlgGraph, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayIntEdge_assert(this.sqlgGraph1, e1);
        }
    }

    private void testBatchArrayIntEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
        e1 = sqlgGraph.traversal().E(e1.id()).next();
        int[] array = e1.value("array");
        assertArrayEquals(new int[]{2, 1}, array);
    }

    @Test
    public void testBatchArrayIntegerEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new Integer[]{2, 1});
        this.sqlgGraph.tx().commit();
        testBatchArrayIntegerEdge_assert(this.sqlgGraph, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayIntegerEdge_assert(this.sqlgGraph1, e1);
        }
    }

    private void testBatchArrayIntegerEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
        e1 = sqlgGraph.traversal().E(e1.id()).next();
        Integer[] array = e1.value("array");
        assertArrayEquals(new Integer[]{2, 1}, array);
    }

    @Test
    public void testBatchArrayboolean() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new boolean[]{true, false});
        this.sqlgGraph.tx().commit();
        testBatchArrayboolean_assert(this.sqlgGraph, god);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayboolean_assert(this.sqlgGraph1, god);
        }
    }

    private void testBatchArrayboolean_assert(SqlgGraph sqlgGraph, Vertex god) {
        god = sqlgGraph.traversal().V(god.id()).next();
        boolean[] array = god.value("array");
        assertArrayEquals(new boolean[]{true, false}, array);
    }

    @Test
    public void testBatchArrayBoolean() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Boolean[]{true, false});
        this.sqlgGraph.tx().commit();
        testBatchArrayBoolean_assert(this.sqlgGraph, god);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayBoolean_assert(this.sqlgGraph1, god);
        }
    }

    private void testBatchArrayBoolean_assert(SqlgGraph sqlgGraph, Vertex god) {
        god = sqlgGraph.traversal().V(god.id()).next();
        Boolean[] array = god.value("array");
        assertArrayEquals(new Boolean[]{true, false}, array);
    }

    @Test
    public void testBatchArraybooleanEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new boolean[]{true, false});
        this.sqlgGraph.tx().commit();
        testBatchArraybooleanEdge_assert(this.sqlgGraph, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArraybooleanEdge_assert(this.sqlgGraph1, e1);
        }
    }

    private void testBatchArraybooleanEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
        e1 = sqlgGraph.traversal().E(e1.id()).next();
        boolean[] array = e1.value("array");
        assertArrayEquals(new boolean[]{true, false}, array);
    }

    @Test
    public void testBatchArrayBooleanEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new Boolean[]{true, false});
        this.sqlgGraph.tx().commit();
        testBatchArrayBooleanEdge_assert(this.sqlgGraph, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayBooleanEdge_assert(this.sqlgGraph1, e1);
        }
    }

    private void testBatchArrayBooleanEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
        e1 = sqlgGraph.traversal().E(e1.id()).next();
        Boolean[] array = e1.value("array");
        assertArrayEquals(new Boolean[]{true, false}, array);
    }

    @Test
    public void testBatchArraybyte() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new byte[]{1, 3});
        this.sqlgGraph.tx().commit();
        testBatchArraybyte_assert(this.sqlgGraph, god);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArraybyte_assert(this.sqlgGraph1, god);
        }
    }

    private void testBatchArraybyte_assert(SqlgGraph sqlgGraph, Vertex god) {
        god = sqlgGraph.traversal().V(god.id()).next();
        byte[] array = god.value("array");
        assertArrayEquals(new byte[]{1, 3}, array);
    }

    @Test
    public void testBatchArrayByte() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Byte[] bytes = {1, 3};
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", bytes);
        this.sqlgGraph.tx().commit();
        testBatchArrayByte_assert(this.sqlgGraph, god);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayByte_assert(this.sqlgGraph1, god);
        }
    }

    private void testBatchArrayByte_assert(SqlgGraph sqlgGraph, Vertex god) {
        god = sqlgGraph.traversal().V(god.id()).next();
        Byte[] array = god.value("array");
        assertArrayEquals(new Byte[]{1, 3}, array);
    }

    @Test
    public void testBatchArraybyteEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new byte[]{1, 3});
        this.sqlgGraph.tx().commit();
        testbatchArraybyteEdge_assert(this.sqlgGraph, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testbatchArraybyteEdge_assert(this.sqlgGraph1, e1);
        }
    }

    private void testbatchArraybyteEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
        e1 = sqlgGraph.traversal().E(e1.id()).next();
        byte[] array = e1.value("array");
        assertArrayEquals(new byte[]{1, 3}, array);
    }

    @Test
    public void testBatchArrayByteEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Byte[] bytes = {1, 3};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", bytes);
        this.sqlgGraph.tx().commit();
        testBatchArrayByteEdge_assert(this.sqlgGraph, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayByteEdge_assert(this.sqlgGraph1, e1);
        }
    }

    private void testBatchArrayByteEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
        e1 = sqlgGraph.traversal().E(e1.id()).next();
        Byte[] array = e1.value("array");
        assertArrayEquals(new Byte[]{1, 3}, array);
    }

    @Test
    public void testBatchArraylong() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new long[]{1L, 3L});
        this.sqlgGraph.tx().commit();
        testBatchArraylong_assert(this.sqlgGraph, god);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArraylong_assert(this.sqlgGraph1, god);
        }
    }

    private void testBatchArraylong_assert(SqlgGraph sqlgGraph, Vertex god) {
        god = sqlgGraph.traversal().V(god.id()).next();
        long[] array = god.value("array");
        assertArrayEquals(new long[]{1L, 3L}, array);
    }

    @Test
    public void testBatchArrayLong() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Long[] longs = {1L, 3L};
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", longs);
        this.sqlgGraph.tx().commit();
        testBatchArrayLong_assert(this.sqlgGraph, god);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayLong_assert(this.sqlgGraph1, god);
        }
    }

    private void testBatchArrayLong_assert(SqlgGraph sqlgGraph, Vertex god) {
        god = sqlgGraph.traversal().V(god.id()).next();
        Long[] array = god.value("array");
        assertArrayEquals(new Long[]{1L, 3L}, array);
    }

    @Test
    public void testBatchArraylongEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new long[]{1L, 3L});
        this.sqlgGraph.tx().commit();
        testBatchArraylongEdge_assert(this.sqlgGraph, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArraylongEdge_assert(this.sqlgGraph1, e1);
        }
    }

    private void testBatchArraylongEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
        e1 = sqlgGraph.traversal().E(e1.id()).next();
        long[] array = e1.value("array");
        assertArrayEquals(new long[]{1L, 3L}, array);
    }

    @Test
    public void testBatchArrayLongEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Long[] longs = {1L, 3L};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", longs);
        this.sqlgGraph.tx().commit();
        testBatchArrayLongEdge_assert(this.sqlgGraph, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayLongEdge_assert(this.sqlgGraph1, e1);
        }
    }

    private void testBatchArrayLongEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
        e1 = sqlgGraph.traversal().E(e1.id()).next();
        Long[] array = e1.value("array");
        assertArrayEquals(new Long[]{1L, 3L}, array);
    }

    @Test
    public void testBatchArrayfloat() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new float[]{1.1f, 3.3f});
        this.sqlgGraph.tx().commit();
        testBatchArrayfloat_assert(this.sqlgGraph, god);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayfloat_assert(this.sqlgGraph1, god);
        }
    }

    private void testBatchArrayfloat_assert(SqlgGraph sqlgGraph, Vertex god) {
        god = sqlgGraph.traversal().V(god.id()).next();
        float[] array = god.value("array");
        assertArrayEquals(new float[]{1.1f, 3.3f}, array, 0f);
    }

    @Test
    public void testBatchArrayFloat() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Float[]{1.1f, 2.2f});
        this.sqlgGraph.tx().commit();
        testBatchArrayFloat_assert(this.sqlgGraph, god);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayFloat_assert(this.sqlgGraph1, god);
        }
    }

    private void testBatchArrayFloat_assert(SqlgGraph sqlgGraph, Vertex god) {
        god = sqlgGraph.traversal().V(god.id()).next();
        Float[] array = god.value("array");
        assertArrayEquals(new Float[]{1.1f, 2.2f}, array);
    }

    @Test
    public void testBatchArrayfloatEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new float[]{1.1f, 3.3f});
        this.sqlgGraph.tx().commit();
        testBatchArrayfloatEdge_assert(this.sqlgGraph, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayfloatEdge_assert(this.sqlgGraph1, e1);
        }
    }

    private void testBatchArrayfloatEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
        e1 = sqlgGraph.traversal().E(e1.id()).next();
        float[] array = e1.value("array");
        assertArrayEquals(new float[]{1.1f, 3.3f}, array, 0f);
    }

    @Test
    public void testBatchArrayFloatEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new Float[]{1.1f, 2.2f});
        this.sqlgGraph.tx().commit();
        testBatchArrayFloatEdge_assert(this.sqlgGraph, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayFloatEdge_assert(this.sqlgGraph1, e1);
        }
    }

    private void testBatchArrayFloatEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
        e1 = sqlgGraph.traversal().E(e1.id()).next();
        Float[] array = e1.value("array");
        assertArrayEquals(new Float[]{1.1f, 2.2f}, array);
    }

    @Test
    public void testBatchArraydouble() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new double[]{1.1d, 3.3d});
        this.sqlgGraph.tx().commit();
        testBatchArraydouble_assert(this.sqlgGraph, god);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArraydouble_assert(this.sqlgGraph1, god);
        }
    }

    private void testBatchArraydouble_assert(SqlgGraph sqlgGraph, Vertex god) {
        god = sqlgGraph.traversal().V(god.id()).next();
        double[] array = god.value("array");
        assertArrayEquals(new double[]{1.1d, 3.3d}, array, 0d);
    }

    @Test
    public void testBatchArrayDouble() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Double[]{1.1d, 2.2d});
        this.sqlgGraph.tx().commit();
        testBatchArrayDouble_assert(this.sqlgGraph, god);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayDouble_assert(this.sqlgGraph1, god);
        }
    }

    private void testBatchArrayDouble_assert(SqlgGraph sqlgGraph, Vertex god) {
        god = sqlgGraph.traversal().V(god.id()).next();
        Double[] array = god.value("array");
        assertArrayEquals(new Double[]{1.1d, 2.2d}, array);
    }

    @Test
    public void testBatchArraydoubleEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new double[]{1.1d, 3.3d});
        this.sqlgGraph.tx().commit();
        testBatchArraydoubleEdge_assert(this.sqlgGraph, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArraydoubleEdge_assert(this.sqlgGraph1, e1);
        }
    }

    private void testBatchArraydoubleEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
        e1 = sqlgGraph.traversal().E(e1.id()).next();
        double[] array = e1.value("array");
        assertArrayEquals(new double[]{1.1d, 3.3d}, array, 0d);
    }

    @Test
    public void testBatchArrayDoubleEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge e1 = v1.addEdge("test", v2, "array", new Double[]{1.1d, 2.2d});
        this.sqlgGraph.tx().commit();
        testBatchArrayDoubleEdge_assert(this.sqlgGraph, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchArrayDoubleEdge_assert(this.sqlgGraph1, e1);
        }
    }

    private void testBatchArrayDoubleEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
        e1 = sqlgGraph.traversal().E(e1.id()).next();
        Double[] array = e1.value("array");
        assertArrayEquals(new Double[]{1.1d, 2.2d}, array);
    }

}
