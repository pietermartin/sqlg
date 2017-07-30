package org.umlg.sqlg.test.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * Date: 2016/05/22
 * Time: 9:09 AM
 */
@RunWith(Parameterized.class)
public class TestBatchNormalPrimitiveArrays extends BaseTest {

    @Parameterized.Parameter
    public Object value;

    private static ObjectMapper objectMapper = new ObjectMapper();

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Parameterized.Parameters(name = "{index}: value:{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {new String[]{"asdasd", "asdasd"}},
                {new boolean[]{false, true}}, {new Boolean[]{Boolean.TRUE, Boolean.FALSE}}, {new byte[]{(byte) 1, (byte) 2}},
                {new short[]{(short) 1, (short) 2}}, {new Short[]{(short) 1, (short) 2}}, {new int[]{1, 1}}, {new Integer[]{1, 1}},
                {new long[]{1L, 2L}}, {new Long[]{1L, 2L}}, {new double[]{2D, 1D}}, {new Double[]{2D, 3D}}
        });
    }

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
        if (this.value instanceof String[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStringArrayValues());
        }
        if (this.value instanceof byte[] || this.value instanceof Byte[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsByteArrayValues());
        }
        if (this.value instanceof boolean[] || this.value instanceof Boolean[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBooleanArrayValues());
        }
        if (this.value instanceof short[] || this.value instanceof Short[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsShortArrayValues());
        }
        if (this.value instanceof int[] || this.value instanceof Integer[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
        }
        if (this.value instanceof long[] || this.value instanceof Long[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLongArrayValues());
        }
        if (this.value instanceof float[] || this.value instanceof Float[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatArrayValues());
        }
        if (this.value instanceof double[] || this.value instanceof Double[]) {
            Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDoubleArrayValues());
        }
    }

    @Test
    public void testPrimitiveArray() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "array", this.value);
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Edge e = a.addEdge("ab", b, "array", this.value);
        this.sqlgGraph.tx().commit();
        a = this.sqlgGraph.traversal().V(a.id()).next();
        Object array = a.value("array");
        Class<?> clazz = array.getClass().getComponentType();
        test(array, clazz);

        e = this.sqlgGraph.traversal().E(e.id()).next();
        array = e.value("array");
        clazz = array.getClass().getComponentType();
        test(array, clazz);

        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);

            a = this.sqlgGraph1.traversal().V(a.id()).next();
            array = a.value("array");
            clazz = array.getClass().getComponentType();
            test(array, clazz);

            e = this.sqlgGraph1.traversal().E(e.id()).next();
            array = e.value("array");
            clazz = array.getClass().getComponentType();
            test(array, clazz);
        }

    }

    private void test(Object array, Class<?> clazz) {
        if (clazz.equals(String.class)) {
            Assert.assertArrayEquals((String[]) this.value, (String[]) array);
        } else if (clazz.equals(boolean.class)) {
            Assert.assertArrayEquals((boolean[]) this.value, (boolean[]) array);
        } else if (clazz.equals(Boolean.class)) {
            Assert.assertArrayEquals((Boolean[]) this.value, (Boolean[]) array);
        } else if (clazz.equals(byte.class)) {
            Assert.assertArrayEquals((byte[]) this.value, (byte[]) array);
        } else if (clazz.equals(Byte.class)) {
            Assert.assertArrayEquals((Byte[]) this.value, (Byte[]) array);
        } else if (clazz.equals(short.class)) {
            Assert.assertArrayEquals((short[]) this.value, (short[]) array);
        } else if (clazz.equals(Short.class)) {
            Assert.assertArrayEquals((Short[]) this.value, (Short[]) array);
        } else if (clazz.equals(int.class)) {
            Assert.assertArrayEquals((int[]) this.value, (int[]) array);
        } else if (clazz.equals(Integer.class)) {
            Assert.assertArrayEquals((Integer[]) this.value, (Integer[]) array);
        } else if (clazz.equals(long.class)) {
            Assert.assertArrayEquals((long[]) this.value, (long[]) array);
        } else if (clazz.equals(Long.class)) {
            Assert.assertArrayEquals((Long[]) this.value, (Long[]) array);
        } else if (clazz.equals(float.class)) {
            Assert.assertArrayEquals((float[]) this.value, (float[]) array, 0);
        } else if (clazz.equals(Float.class)) {
            Assert.assertArrayEquals((Float[]) this.value, (Float[]) array);
        } else if (clazz.equals(double.class)) {
            Assert.assertArrayEquals((double[]) this.value, (double[]) array, 0);
        } else if (clazz.equals(Double.class)) {
            Assert.assertArrayEquals((Double[]) this.value, (Double[]) array);
        } else {
            Assert.fail("Unknown type");
        }
    }

//    @Test
//    public void testBatchArrayString() throws InterruptedException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStringArrayValues());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new String[]{"a", "b"});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayString_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayString_assert(this.sqlgGraph1, god);
//        }
//    }
//
//    private void testBatchArrayString_assert(SqlgGraph sqlgGraph, Vertex god) {
//        god = sqlgGraph.traversal().V(god.id()).next();
//        String[] array = god.value("array");
//        assertArrayEquals(array, new String[]{"a", "b"});
//    }
//
//    @Test
//    public void testBatchArrayStringEdge() throws InterruptedException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStringArrayValues());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Edge e1 = v1.addEdge("test", v2, "array", new String[]{"a", "b"});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayStringEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayStringEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testBatchArrayStringEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        String[] array = e1.value("array");
//        assertArrayEquals(array, new String[]{"a", "b"});
//    }
//
//    @Test
//    public void testBatchArrayshort() throws InterruptedException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsShortArrayValues());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new short[]{2, 1});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayshort_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayshort_assert(this.sqlgGraph1, god);
//        }
//    }
//
//    private void testBatchArrayshort_assert(SqlgGraph sqlgGraph, Vertex god) {
//        god = sqlgGraph.traversal().V(god.id()).next();
//        short[] array = god.value("array");
//        assertArrayEquals(new short[]{2, 1}, array);
//    }
//
//    @Test
//    public void testBatchArrayShort() throws InterruptedException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsShortArrayValues());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Short[]{2, 1});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayShort_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayShort_assert(this.sqlgGraph1, god);
//        }
//    }
//
//    private void testBatchArrayShort_assert(SqlgGraph sqlgGraph, Vertex god) {
//        god = sqlgGraph.traversal().V(god.id()).next();
//        Short[] array = god.value("array");
//        assertArrayEquals(new Short[]{2, 1}, array);
//    }
//
//    @Test
//    public void testBatchArrayshortEdge() throws InterruptedException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsShortArrayValues());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Edge e1 = v1.addEdge("test", v2, "array", new short[]{2, 1});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayshortEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayshortEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testBatchArrayshortEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        short[] array = e1.value("array");
//        assertArrayEquals(new short[]{2, 1}, array);
//    }
//
//    @Test
//    public void testBatchArrayShortEdge() throws InterruptedException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsShortArrayValues());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Edge e1 = v1.addEdge("test", v2, "array", new Short[]{2, 1});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayShortEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayShortEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testBatchArrayShortEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        Short[] array = e1.value("array");
//        assertArrayEquals(new Short[]{2, 1}, array);
//    }
//
//    @Test
//    public void testBatchArrayInt() throws InterruptedException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new int[]{2, 1});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayInt_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayInt_assert(this.sqlgGraph1, god);
//        }
//    }
//
//    private void testBatchArrayInt_assert(SqlgGraph sqlgGraph, Vertex god) {
//        god = sqlgGraph.traversal().V(god.id()).next();
//        int[] array = god.value("array");
//        assertArrayEquals(new int[]{2, 1}, array);
//    }
//
//    @Test
//    public void testBatchArrayInteger() throws InterruptedException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Integer[]{2, 1});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayInteger_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayInteger_assert(this.sqlgGraph1, god);
//        }
//    }
//
//    private void testBatchArrayInteger_assert(SqlgGraph sqlgGraph, Vertex god) {
//        god = sqlgGraph.traversal().V(god.id()).next();
//        Integer[] array = god.value("array");
//        assertArrayEquals(new Integer[]{2, 1}, array);
//    }
//
//    @Test
//    public void testBatchArrayIntEdge() throws InterruptedException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Edge e1 = v1.addEdge("test", v2, "array", new int[]{2, 1});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayIntEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayIntEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testBatchArrayIntEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        int[] array = e1.value("array");
//        assertArrayEquals(new int[]{2, 1}, array);
//    }
//
//    @Test
//    public void testBatchArrayIntegerEdge() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Edge e1 = v1.addEdge("test", v2, "array", new Integer[]{2, 1});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayIntegerEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayIntegerEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testBatchArrayIntegerEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        Integer[] array = e1.value("array");
//        assertArrayEquals(new Integer[]{2, 1}, array);
//    }
//
//    @Test
//    public void testBatchArrayboolean() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new boolean[]{true, false});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayboolean_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayboolean_assert(this.sqlgGraph1, god);
//        }
//    }
//
//    private void testBatchArrayboolean_assert(SqlgGraph sqlgGraph, Vertex god) {
//        god = sqlgGraph.traversal().V(god.id()).next();
//        boolean[] array = god.value("array");
//        assertArrayEquals(new boolean[]{true, false}, array);
//    }
//
//    @Test
//    public void testBatchArrayBoolean() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Boolean[]{true, false});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayBoolean_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayBoolean_assert(this.sqlgGraph1, god);
//        }
//    }
//
//    private void testBatchArrayBoolean_assert(SqlgGraph sqlgGraph, Vertex god) {
//        god = sqlgGraph.traversal().V(god.id()).next();
//        Boolean[] array = god.value("array");
//        assertArrayEquals(new Boolean[]{true, false}, array);
//    }
//
//    @Test
//    public void testBatchArraybooleanEdge() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Edge e1 = v1.addEdge("test", v2, "array", new boolean[]{true, false});
//        this.sqlgGraph.tx().commit();
//        testBatchArraybooleanEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArraybooleanEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testBatchArraybooleanEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        boolean[] array = e1.value("array");
//        assertArrayEquals(new boolean[]{true, false}, array);
//    }
//
//    @Test
//    public void testBatchArrayBooleanEdge() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Edge e1 = v1.addEdge("test", v2, "array", new Boolean[]{true, false});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayBooleanEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayBooleanEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testBatchArrayBooleanEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        Boolean[] array = e1.value("array");
//        assertArrayEquals(new Boolean[]{true, false}, array);
//    }
//
//    @Test
//    public void testBatchArraybyte() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new byte[]{1,3});
//        this.sqlgGraph.tx().commit();
//        testBatchArraybyte_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArraybyte_assert(this.sqlgGraph1, god);
//        }
//        this.sqlgGraph.tx().normalBatchModeOn();
//        god.property("array", "I pack some weirdness:'\",:/?".getBytes(StandardCharsets.UTF_8));
//        this.sqlgGraph.tx().commit();
//        testBatchArraybyteSpecial_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArraybyteSpecial_assert(this.sqlgGraph1, god);
//        }
//    }
//
//    private void testBatchArraybyte_assert(SqlgGraph sqlgGraph, Vertex god) {
//        god = sqlgGraph.traversal().V(god.id()).next();
//        byte[] array = god.value("array");
//        assertArrayEquals(new byte[]{1,3}, array);
//    }
//
//    @Test
//    public void testBatchArraybyteSpecial() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", "I pack some weirdness:'\",:/?".getBytes(StandardCharsets.UTF_8));
//        this.sqlgGraph.tx().commit();
//        testBatchArraybyteSpecial_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArraybyteSpecial_assert(this.sqlgGraph1, god);
//        }
//    }
//
//    private void testBatchArraybyteSpecial_assert(SqlgGraph sqlgGraph, Vertex god) {
//        god = sqlgGraph.traversal().V(god.id()).next();
//        byte[] array = god.value("array");
//        assertEquals("I pack some weirdness:'\",:/?", new String(array,StandardCharsets.UTF_8));
//    }
//
//    @Test
//    public void testBatchArrayByte() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Byte[] bytes = {1, 3};
//        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", bytes);
//        this.sqlgGraph.tx().commit();
//        testBatchArrayByte_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayByte_assert(this.sqlgGraph1, god);
//        }
//    }
//
//    private void testBatchArrayByte_assert(SqlgGraph sqlgGraph, Vertex god) {
//        god = sqlgGraph.traversal().V(god.id()).next();
//        Byte[] array = god.value("array");
//        assertArrayEquals(new Byte[]{1, 3}, array);
//    }
//
//    @Test
//    public void testBatchArraybyteEdge() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Edge e1 = v1.addEdge("test", v2, "array", new byte[]{1, 3});
//        this.sqlgGraph.tx().commit();
//        testbatchArraybyteEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testbatchArraybyteEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testbatchArraybyteEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        byte[] array = e1.value("array");
//        assertArrayEquals(new byte[]{1, 3}, array);
//    }
//
//    @Test
//    public void testBatchArrayByteEdge() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Byte[] bytes = {1, 3};
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Edge e1 = v1.addEdge("test", v2, "array", bytes);
//        this.sqlgGraph.tx().commit();
//        testBatchArrayByteEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayByteEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testBatchArrayByteEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        Byte[] array = e1.value("array");
//        assertArrayEquals(new Byte[]{1, 3}, array);
//    }
//
//    @Test
//    public void testBatchArraylong() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new long[]{1L, 3L});
//        this.sqlgGraph.tx().commit();
//        testBatchArraylong_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArraylong_assert(this.sqlgGraph1, god);
//        }
//    }
//
//    private void testBatchArraylong_assert(SqlgGraph sqlgGraph, Vertex god) {
//        god = sqlgGraph.traversal().V(god.id()).next();
//        long[] array = god.value("array");
//        assertArrayEquals(new long[]{1L, 3L}, array);
//    }
//
//    @Test
//    public void testBatchArrayLong() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Long[] longs = {1L, 3L};
//        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", longs);
//        this.sqlgGraph.tx().commit();
//        testBatchArrayLong_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayLong_assert(this.sqlgGraph1, god);
//        }
//    }
//
//    private void testBatchArrayLong_assert(SqlgGraph sqlgGraph, Vertex god) {
//        god = sqlgGraph.traversal().V(god.id()).next();
//        Long[] array = god.value("array");
//        assertArrayEquals(new Long[]{1L, 3L}, array);
//    }
//
//    @Test
//    public void testBatchArraylongEdge() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Edge e1 = v1.addEdge("test", v2, "array", new long[]{1L, 3L});
//        this.sqlgGraph.tx().commit();
//        testBatchArraylongEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArraylongEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testBatchArraylongEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        long[] array = e1.value("array");
//        assertArrayEquals(new long[]{1L, 3L}, array);
//    }
//
//    @Test
//    public void testBatchArrayLongEdge() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Long[] longs = {1L, 3L};
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Edge e1 = v1.addEdge("test", v2, "array", longs);
//        this.sqlgGraph.tx().commit();
//        testBatchArrayLongEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayLongEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testBatchArrayLongEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        Long[] array = e1.value("array");
//        assertArrayEquals(new Long[]{1L, 3L}, array);
//    }
//
//    @Test
//    public void testBatchArrayfloat() throws InterruptedException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatArrayValues());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new float[]{1.1f, 3.3f});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayfloat_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayfloat_assert(this.sqlgGraph1, god);
//        }
//    }
//
//    private void testBatchArrayfloat_assert(SqlgGraph sqlgGraph, Vertex god) {
//        god = sqlgGraph.traversal().V(god.id()).next();
//        float[] array = god.value("array");
//        assertArrayEquals(new float[]{1.1f, 3.3f}, array, 0f);
//    }
//
//    @Test
//    public void testBatchArrayFloat() throws InterruptedException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Float[]{1.1f, 2.2f});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayFloat_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayFloat_assert(this.sqlgGraph1, god);
//        }
//    }
//
//    private void testBatchArrayFloat_assert(SqlgGraph sqlgGraph, Vertex god) {
//        god = sqlgGraph.traversal().V(god.id()).next();
//        Float[] array = god.value("array");
//        assertArrayEquals(new Float[]{1.1f, 2.2f}, array);
//    }
//
//    @Test
//    public void testBatchArrayfloatEdge() throws InterruptedException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatArrayValues());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Edge e1 = v1.addEdge("test", v2, "array", new float[]{1.1f, 3.3f});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayfloatEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayfloatEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testBatchArrayfloatEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        float[] array = e1.value("array");
//        assertArrayEquals(new float[]{1.1f, 3.3f}, array, 0f);
//    }
//
//    @Test
//    public void testBatchArrayFloatEdge() throws InterruptedException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatArrayValues());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Edge e1 = v1.addEdge("test", v2, "array", new Float[]{1.1f, 2.2f});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayFloatEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayFloatEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testBatchArrayFloatEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        Float[] array = e1.value("array");
//        assertArrayEquals(new Float[]{1.1f, 2.2f}, array);
//    }
//
//    @Test
//    public void testBatchArraydouble() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new double[]{1.1d, 3.3d});
//        this.sqlgGraph.tx().commit();
//        testBatchArraydouble_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArraydouble_assert(this.sqlgGraph1, god);
//        }
//    }
//
//    private void testBatchArraydouble_assert(SqlgGraph sqlgGraph, Vertex god) {
//        god = sqlgGraph.traversal().V(god.id()).next();
//        double[] array = god.value("array");
//        assertArrayEquals(new double[]{1.1d, 3.3d}, array, 0d);
//    }
//
//    @Test
//    public void testBatchArrayDouble() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new Double[]{1.1d, 2.2d});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayDouble_assert(this.sqlgGraph, god);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayDouble_assert(this.sqlgGraph1, god);
//        }
//    }
//
//    private void testBatchArrayDouble_assert(SqlgGraph sqlgGraph, Vertex god) {
//        god = sqlgGraph.traversal().V(god.id()).next();
//        Double[] array = god.value("array");
//        assertArrayEquals(new Double[]{1.1d, 2.2d}, array);
//    }
//
//    @Test
//    public void testBatchArraydoubleEdge() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Edge e1 = v1.addEdge("test", v2, "array", new double[]{1.1d, 3.3d});
//        this.sqlgGraph.tx().commit();
//        testBatchArraydoubleEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArraydoubleEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testBatchArraydoubleEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        double[] array = e1.value("array");
//        assertArrayEquals(new double[]{1.1d, 3.3d}, array, 0d);
//    }
//
//    @Test
//    public void testBatchArrayDoubleEdge() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "GOD");
//        Edge e1 = v1.addEdge("test", v2, "array", new Double[]{1.1d, 2.2d});
//        this.sqlgGraph.tx().commit();
//        testBatchArrayDoubleEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testBatchArrayDoubleEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testBatchArrayDoubleEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        Double[] array = e1.value("array");
//        assertArrayEquals(new Double[]{1.1d, 2.2d}, array);
//    }

}
