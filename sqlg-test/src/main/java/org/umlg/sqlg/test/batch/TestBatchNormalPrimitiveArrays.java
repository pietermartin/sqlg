package org.umlg.sqlg.test.batch;

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

import static org.junit.Assert.assertArrayEquals;

/**
 * Date: 2016/05/22
 * Time: 9:09 AM
 */
@RunWith(Parameterized.class)
public class TestBatchNormalPrimitiveArrays extends BaseTest {

    @Parameterized.Parameter
    public Object value;

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
            assertArrayEquals((String[]) this.value, (String[]) array);
        } else if (clazz.equals(boolean.class)) {
            assertArrayEquals((boolean[]) this.value, (boolean[]) array);
        } else if (clazz.equals(Boolean.class)) {
            assertArrayEquals((Boolean[]) this.value, (Boolean[]) array);
        } else if (clazz.equals(byte.class)) {
            assertArrayEquals((byte[]) this.value, (byte[]) array);
        } else if (clazz.equals(Byte.class)) {
            assertArrayEquals((Byte[]) this.value, (Byte[]) array);
        } else if (clazz.equals(short.class)) {
            assertArrayEquals((short[]) this.value, (short[]) array);
        } else if (clazz.equals(Short.class)) {
            assertArrayEquals((Short[]) this.value, (Short[]) array);
        } else if (clazz.equals(int.class)) {
            assertArrayEquals((int[]) this.value, (int[]) array);
        } else if (clazz.equals(Integer.class)) {
            assertArrayEquals((Integer[]) this.value, (Integer[]) array);
        } else if (clazz.equals(long.class)) {
            assertArrayEquals((long[]) this.value, (long[]) array);
        } else if (clazz.equals(Long.class)) {
            assertArrayEquals((Long[]) this.value, (Long[]) array);
        } else if (clazz.equals(float.class)) {
            assertArrayEquals((float[]) this.value, (float[]) array, 0);
        } else if (clazz.equals(Float.class)) {
            assertArrayEquals((Float[]) this.value, (Float[]) array);
        } else if (clazz.equals(double.class)) {
            assertArrayEquals((double[]) this.value, (double[]) array, 0);
        } else if (clazz.equals(Double.class)) {
            assertArrayEquals((Double[]) this.value, (Double[]) array);
        } else {
            Assert.fail("Unknown type");
        }
    }

}
