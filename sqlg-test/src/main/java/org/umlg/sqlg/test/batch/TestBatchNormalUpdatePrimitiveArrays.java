package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2016/05/22
 * Time: 9:16 AM
 */
public class TestBatchNormalUpdatePrimitiveArrays extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testStringArrayUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        String[] stringArray = new String[]{"a", "b"};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "stringArray1", stringArray);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "stringArray2", stringArray);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "stringArray3", stringArray);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        String[] localDateTimeAgain = new String[]{"c", "d"};
        a1.property("stringArray1", localDateTimeAgain);
        a2.property("stringArray2", localDateTimeAgain);
        a3.property("stringArray3", localDateTimeAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertArrayEquals(localDateTimeAgain, a1.value("stringArray1"));
        Assert.assertFalse(a1.property("stringArray2").isPresent());
        Assert.assertFalse(a1.property("stringArray3").isPresent());

        Assert.assertFalse(a2.property("stringArray1").isPresent());
        Assert.assertArrayEquals(localDateTimeAgain, a2.value("stringArray2"));
        Assert.assertFalse(a2.property("stringArray3").isPresent());

        Assert.assertFalse(a3.property("stringArray1").isPresent());
        Assert.assertFalse(a3.property("stringArray2").isPresent());
        Assert.assertArrayEquals(localDateTimeAgain, a3.value("stringArray3"));
    }

    @Test
    public void testshortArrayUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        short[] shortArray = new short[]{1, 2};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "shortArray1", shortArray);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "shortArray2", shortArray);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "shortArray3", shortArray);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        short[] shortArrayAgain = new short[]{3, 4};
        a1.property("shortArray1", shortArrayAgain);
        a2.property("shortArray2", shortArrayAgain);
        a3.property("shortArray3", shortArrayAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertArrayEquals(shortArrayAgain, a1.value("shortArray1"));
        Assert.assertFalse(a1.property("shortArray2").isPresent());
        Assert.assertFalse(a1.property("shortArray3").isPresent());

        Assert.assertFalse(a2.property("shortArray1").isPresent());
        Assert.assertArrayEquals(shortArrayAgain, a2.value("shortArray2"));
        Assert.assertFalse(a2.property("shortArray3").isPresent());

        Assert.assertFalse(a3.property("shortArray1").isPresent());
        Assert.assertFalse(a3.property("shortArray2").isPresent());
        Assert.assertArrayEquals(shortArrayAgain, a3.value("shortArray3"));

    }

    @Test
    public void testShortArrayUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Short[] shortArray = new Short[]{1, 2};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "shortArray1", shortArray);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "shortArray2", shortArray);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "shortArray3", shortArray);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        Short[] shortArrayAgain = new Short[]{3, 4};
        a1.property("shortArray1", shortArrayAgain);
        a2.property("shortArray2", shortArrayAgain);
        a3.property("shortArray3", shortArrayAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertArrayEquals(shortArrayAgain, a1.value("shortArray1"));
        Assert.assertFalse(a1.property("shortArray2").isPresent());
        Assert.assertFalse(a1.property("shortArray3").isPresent());

        Assert.assertFalse(a2.property("shortArray1").isPresent());
        Assert.assertArrayEquals(shortArrayAgain, a2.value("shortArray2"));
        Assert.assertFalse(a2.property("shortArray3").isPresent());

        Assert.assertFalse(a3.property("shortArray1").isPresent());
        Assert.assertFalse(a3.property("shortArray2").isPresent());
        Assert.assertArrayEquals(shortArrayAgain, a3.value("shortArray3"));
    }

    @Test
    public void testintArrayUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        int[] intArray = new int[]{1, 2};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "intArray1", intArray);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "intArray2", intArray);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "intArray3", intArray);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        int[] intArrayAgain = new int[]{3, 4};
        a1.property("intArray1", intArrayAgain);
        a2.property("intArray2", intArrayAgain);
        a3.property("intArray3", intArrayAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertArrayEquals(intArrayAgain, a1.value("intArray1"));
        Assert.assertFalse(a1.property("intArray2").isPresent());
        Assert.assertFalse(a1.property("intArray3").isPresent());

        Assert.assertFalse(a2.property("intArray1").isPresent());
        Assert.assertArrayEquals(intArrayAgain, a2.value("intArray2"));
        Assert.assertFalse(a2.property("intArray3").isPresent());

        Assert.assertFalse(a3.property("intArray1").isPresent());
        Assert.assertFalse(a3.property("intArray2").isPresent());
        Assert.assertArrayEquals(intArrayAgain, a3.value("intArray3"));

    }

    @Test
    public void testIntegerArrayUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Integer[] integerArray = new Integer[]{1, 2};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "integerArray1", integerArray);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "integerArray2", integerArray);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "integerArray3", integerArray);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        Integer[] integerArrayAgain = new Integer[]{3, 4};
        a1.property("integerArray1", integerArrayAgain);
        a2.property("integerArray2", integerArrayAgain);
        a3.property("integerArray3", integerArrayAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertArrayEquals(integerArrayAgain, a1.value("integerArray1"));
        Assert.assertFalse(a1.property("integerArray2").isPresent());
        Assert.assertFalse(a1.property("integerArray3").isPresent());

        Assert.assertFalse(a2.property("integerArray1").isPresent());
        Assert.assertArrayEquals(integerArrayAgain, a2.value("integerArray2"));
        Assert.assertFalse(a2.property("integerArray3").isPresent());

        Assert.assertFalse(a3.property("integerArray1").isPresent());
        Assert.assertFalse(a3.property("integerArray2").isPresent());
        Assert.assertArrayEquals(integerArrayAgain, a3.value("integerArray3"));
    }

    @Test
    public void testbooleanArrayUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        boolean[] booleanArray = new boolean[]{true, true};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "booleanArray1", booleanArray);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "booleanArray2", booleanArray);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "booleanArray3", booleanArray);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        boolean[] booleanArrayAgain = new boolean[]{false, false};
        a1.property("booleanArray1", booleanArrayAgain);
        a2.property("booleanArray2", booleanArrayAgain);
        a3.property("booleanArray3", booleanArrayAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertArrayEquals(booleanArrayAgain, a1.value("booleanArray1"));
        Assert.assertFalse(a1.property("booleanArray2").isPresent());
        Assert.assertFalse(a1.property("booleanArray3").isPresent());

        Assert.assertFalse(a2.property("booleanArray1").isPresent());
        Assert.assertArrayEquals(booleanArrayAgain, a2.value("booleanArray2"));
        Assert.assertFalse(a2.property("booleanArray3").isPresent());

        Assert.assertFalse(a3.property("booleanArray1").isPresent());
        Assert.assertFalse(a3.property("booleanArray2").isPresent());
        Assert.assertArrayEquals(booleanArrayAgain, a3.value("booleanArray3"));

    }

    @Test
    public void testBooleanArrayUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Boolean[] booleanArray = new Boolean[]{true, true};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "booleanArray1", booleanArray);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "booleanArray2", booleanArray);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "booleanArray3", booleanArray);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        Boolean[] booleanArrayAgain = new Boolean[]{false, false};
        a1.property("booleanArray1", booleanArrayAgain);
        a2.property("booleanArray2", booleanArrayAgain);
        a3.property("booleanArray3", booleanArrayAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertArrayEquals(booleanArrayAgain, a1.value("booleanArray1"));
        Assert.assertFalse(a1.property("booleanArray2").isPresent());
        Assert.assertFalse(a1.property("booleanArray3").isPresent());

        Assert.assertFalse(a2.property("booleanArray1").isPresent());
        Assert.assertArrayEquals(booleanArrayAgain, a2.value("booleanArray2"));
        Assert.assertFalse(a2.property("booleanArray3").isPresent());

        Assert.assertFalse(a3.property("booleanArray1").isPresent());
        Assert.assertFalse(a3.property("booleanArray2").isPresent());
        Assert.assertArrayEquals(booleanArrayAgain, a3.value("booleanArray3"));

    }

    @Test
    public void testbyteArrayUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        byte[] byteArray = new byte[]{1, 2};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "byteArray1", byteArray);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "byteArray2", byteArray);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "byteArray3", byteArray);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        byte[] byteArrayAgain = new byte[]{3, 4};
        a1.property("byteArray1", byteArrayAgain);
        a2.property("byteArray2", byteArrayAgain);
        a3.property("byteArray3", byteArrayAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertArrayEquals(byteArrayAgain, a1.value("byteArray1"));
        Assert.assertFalse(a1.property("byteArray2").isPresent());
        Assert.assertFalse(a1.property("byteArray3").isPresent());

        Assert.assertFalse(a2.property("byteArray1").isPresent());
        Assert.assertArrayEquals(byteArrayAgain, a2.value("byteArray2"));
        Assert.assertFalse(a2.property("byteArray3").isPresent());

        Assert.assertFalse(a3.property("byteArray1").isPresent());
        Assert.assertFalse(a3.property("byteArray2").isPresent());
        Assert.assertArrayEquals(byteArrayAgain, a3.value("byteArray3"));

    }

    @Test
    public void testByteArrayUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Byte[] byteArray = new Byte[]{1, 2};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "byteArray1", byteArray);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "byteArray2", byteArray);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "byteArray3", byteArray);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        Byte[] byteArrayAgain = new Byte[]{3, 4};
        a1.property("byteArray1", byteArrayAgain);
        a2.property("byteArray2", byteArrayAgain);
        a3.property("byteArray3", byteArrayAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertArrayEquals(byteArrayAgain, a1.value("byteArray1"));
        Assert.assertFalse(a1.property("byteArray2").isPresent());
        Assert.assertFalse(a1.property("byteArray3").isPresent());

        Assert.assertFalse(a2.property("byteArray1").isPresent());
        Assert.assertArrayEquals(byteArrayAgain, a2.value("byteArray2"));
        Assert.assertFalse(a2.property("byteArray3").isPresent());

        Assert.assertFalse(a3.property("byteArray1").isPresent());
        Assert.assertFalse(a3.property("byteArray2").isPresent());
        Assert.assertArrayEquals(byteArrayAgain, a3.value("byteArray3"));

    }

    @Test
    public void testlongArrayUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        long[] longArray = new long[]{1L, 2L};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "longArray1", longArray);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "longArray2", longArray);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "longArray3", longArray);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        long[] longArrayAgain = new long[]{3L, 4L};
        a1.property("longArray1", longArrayAgain);
        a2.property("longArray2", longArrayAgain);
        a3.property("longArray3", longArrayAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertArrayEquals(longArrayAgain, a1.value("longArray1"));
        Assert.assertFalse(a1.property("longArray2").isPresent());
        Assert.assertFalse(a1.property("longArray3").isPresent());

        Assert.assertFalse(a2.property("longArray1").isPresent());
        Assert.assertArrayEquals(longArrayAgain, a2.value("longArray2"));
        Assert.assertFalse(a2.property("longArray3").isPresent());

        Assert.assertFalse(a3.property("longArray1").isPresent());
        Assert.assertFalse(a3.property("longArray2").isPresent());
        Assert.assertArrayEquals(longArrayAgain, a3.value("longArray3"));

    }

    @Test
    public void testLongArrayUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Long[] longArray = new Long[]{1L, 2L};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "longArray1", longArray);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "longArray2", longArray);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "longArray3", longArray);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        Long[] longArrayAgain = new Long[]{3L, 4L};
        a1.property("longArray1", longArrayAgain);
        a2.property("longArray2", longArrayAgain);
        a3.property("longArray3", longArrayAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertArrayEquals(longArrayAgain, a1.value("longArray1"));
        Assert.assertFalse(a1.property("longArray2").isPresent());
        Assert.assertFalse(a1.property("longArray3").isPresent());

        Assert.assertFalse(a2.property("longArray1").isPresent());
        Assert.assertArrayEquals(longArrayAgain, a2.value("longArray2"));
        Assert.assertFalse(a2.property("longArray3").isPresent());

        Assert.assertFalse(a3.property("longArray1").isPresent());
        Assert.assertFalse(a3.property("longArray2").isPresent());
        Assert.assertArrayEquals(longArrayAgain, a3.value("longArray3"));

    }

    @Test
    public void testfloatArrayUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        float[] floatArray = new float[]{1F, 2F};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "floatArray1", floatArray);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "floatArray2", floatArray);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "floatArray3", floatArray);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        float[] floatArrayAgain = new float[]{3F, 4F};
        a1.property("floatArray1", floatArrayAgain);
        a2.property("floatArray2", floatArrayAgain);
        a3.property("floatArray3", floatArrayAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertArrayEquals(floatArrayAgain, a1.value("floatArray1"), 0F);
        Assert.assertFalse(a1.property("floatArray2").isPresent());
        Assert.assertFalse(a1.property("floatArray3").isPresent());

        Assert.assertFalse(a2.property("floatArray1").isPresent());
        Assert.assertArrayEquals(floatArrayAgain, a2.value("floatArray2"), 0F);
        Assert.assertFalse(a2.property("floatArray3").isPresent());

        Assert.assertFalse(a3.property("floatArray1").isPresent());
        Assert.assertFalse(a3.property("floatArray2").isPresent());
        Assert.assertArrayEquals(floatArrayAgain, a3.value("floatArray3"), 0F);

    }

    @Test
    public void testFloatArrayUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Float[] floatArray = new Float[]{1F, 2F};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "floatArray1", floatArray);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "floatArray2", floatArray);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "floatArray3", floatArray);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        Float[] floatArrayAgain = new Float[]{3F, 4F};
        a1.property("floatArray1", floatArrayAgain);
        a2.property("floatArray2", floatArrayAgain);
        a3.property("floatArray3", floatArrayAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertArrayEquals(floatArrayAgain, a1.value("floatArray1"));
        Assert.assertFalse(a1.property("floatArray2").isPresent());
        Assert.assertFalse(a1.property("floatArray3").isPresent());

        Assert.assertFalse(a2.property("floatArray1").isPresent());
        Assert.assertArrayEquals(floatArrayAgain, a2.value("floatArray2"));
        Assert.assertFalse(a2.property("floatArray3").isPresent());

        Assert.assertFalse(a3.property("floatArray1").isPresent());
        Assert.assertFalse(a3.property("floatArray2").isPresent());
        Assert.assertArrayEquals(floatArrayAgain, a3.value("floatArray3"));

    }

    @Test
    public void testdoubleArrayUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        double[] doubleArray = new double[]{1D, 2D};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "doubleArray1", doubleArray);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "doubleArray2", doubleArray);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "doubleArray3", doubleArray);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        double[] doubleArrayAgain = new double[]{3D, 4D};
        a1.property("doubleArray1", doubleArrayAgain);
        a2.property("doubleArray2", doubleArrayAgain);
        a3.property("doubleArray3", doubleArrayAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertArrayEquals(doubleArrayAgain, a1.value("doubleArray1"), 0D);
        Assert.assertFalse(a1.property("doubleArray2").isPresent());
        Assert.assertFalse(a1.property("doubleArray3").isPresent());

        Assert.assertFalse(a2.property("doubleArray1").isPresent());
        Assert.assertArrayEquals(doubleArrayAgain, a2.value("doubleArray2"), 0D);
        Assert.assertFalse(a2.property("doubleArray3").isPresent());

        Assert.assertFalse(a3.property("doubleArray1").isPresent());
        Assert.assertFalse(a3.property("doubleArray2").isPresent());
        Assert.assertArrayEquals(doubleArrayAgain, a3.value("doubleArray3"), 0D);

    }

    @Test
    public void testDoubleArrayUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Double[] doubleArray = new Double[]{1D, 2D};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "doubleArray1", doubleArray);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "doubleArray2", doubleArray);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "doubleArray3", doubleArray);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        Double[] doubleArrayAgain = new Double[]{3D, 4D};
        a1.property("doubleArray1", doubleArrayAgain);
        a2.property("doubleArray2", doubleArrayAgain);
        a3.property("doubleArray3", doubleArrayAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertArrayEquals(doubleArrayAgain, a1.value("doubleArray1"));
        Assert.assertFalse(a1.property("doubleArray2").isPresent());
        Assert.assertFalse(a1.property("doubleArray3").isPresent());

        Assert.assertFalse(a2.property("doubleArray1").isPresent());
        Assert.assertArrayEquals(doubleArrayAgain, a2.value("doubleArray2"));
        Assert.assertFalse(a2.property("doubleArray3").isPresent());

        Assert.assertFalse(a3.property("doubleArray1").isPresent());
        Assert.assertFalse(a3.property("doubleArray2").isPresent());
        Assert.assertArrayEquals(doubleArrayAgain, a3.value("doubleArray3"));

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
        Assert.assertArrayEquals(array, new String[]{"c", "d"});
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
        Assert.assertArrayEquals(new short[]{4, 5}, array);
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
        Assert.assertArrayEquals(new Short[]{4,5}, array);
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
        Assert.assertArrayEquals(new int[]{1, 2}, array);
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
        Assert.assertArrayEquals(new Integer[]{1, 2}, array);
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
        Assert.assertArrayEquals(new boolean[]{false, true}, array);
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
        Assert.assertArrayEquals(new Boolean[]{false, true}, array);
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
        Assert.assertArrayEquals(new byte[]{5, 6}, array);
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
        Assert.assertArrayEquals(new Byte[]{6, 7}, array);
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
        Assert.assertArrayEquals(new long[]{5L, 6L}, array);
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
        Assert.assertArrayEquals(new Long[]{6L, 7L}, array);
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
        Assert.assertArrayEquals(new float[]{5.5f, 6.6f}, array, 0f);
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
        Assert.assertArrayEquals(new Float[]{6.6f, 7.7f}, array);
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
        Assert.assertArrayEquals(new double[]{5.5d, 6.6d}, array, 0d);
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
        Assert.assertArrayEquals(new Double[]{6.6d, 7.7d}, array);
    }
}
