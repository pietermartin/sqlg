package org.umlg.sqlg.test.gremlincompile;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Date: 2015/10/07
 * Time: 7:28 PM
 */
public class TestBulkWithin extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestBulkWithin.class.getName());

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Test
    public void testBulkWithinString() {
        for (int i = 0; i < 3; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "name", "a" + i);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within("a0", "a1", "a2")).toList();
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testBulkWithin_StringArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStringArrayValues());
        String[] stringArray1 = new String[]{"a", "b", "c"};
        String[] stringArray2 = new String[]{"d", "e", "f"};
        String[] stringArray3 = new String[]{"g", "h", "i"};
        String[] stringArray4 = new String[]{"j", "k", "l"};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", stringArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", stringArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", stringArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", stringArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(stringArray1, stringArray3, stringArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithinBoolean() {
        this.sqlgGraph.addVertex(T.label, "A", "name", false);
        this.sqlgGraph.addVertex(T.label, "A", "name", true);
        this.sqlgGraph.addVertex(T.label, "A", "name", true);
        this.sqlgGraph.addVertex(T.label, "A", "name", true);
        this.sqlgGraph.addVertex(T.label, "A", "name", true);
        this.sqlgGraph.addVertex(T.label, "A", "name", true);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(true, true, true, true)).toList();
        Assert.assertEquals(5, vertices.size());
    }

    @Test
    public void testBulkWithin_booleanArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBooleanArrayValues());
        boolean[] booleanArray1 = new boolean[]{true, false, true};
        boolean[] booleanArray2 = new boolean[]{false, false, false};
        boolean[] booleanArray3 = new boolean[]{true, false, false};
        boolean[] booleanArray4 = new boolean[]{false, false, true};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", booleanArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", booleanArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", booleanArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", booleanArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(booleanArray1, booleanArray3, booleanArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithin_BooleanArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBooleanArrayValues());
        Boolean[] booleanArray1 = new Boolean[]{true, false, true};
        Boolean[] booleanArray2 = new Boolean[]{false, false, false};
        Boolean[] booleanArray3 = new Boolean[]{true, false, false};
        Boolean[] booleanArray4 = new Boolean[]{false, false, true};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", booleanArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", booleanArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", booleanArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", booleanArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(booleanArray1, booleanArray3, booleanArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithinByte() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsByteValues());
        this.sqlgGraph.addVertex(T.label, "A", "name", (byte)1);
        this.sqlgGraph.addVertex(T.label, "A", "name", (byte)2);
        this.sqlgGraph.addVertex(T.label, "A", "name", (byte)3);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within((byte)1, (byte)2, (byte)3)).toList();
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testBulkWithinByteArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsByteArrayValues());
        Byte[] byteArray1 = new Byte[]{(byte) 1, (byte) 2};
        Byte[] byteArray2 = new Byte[]{(byte) 3, (byte) 4};
        Byte[] byteArray3 = new Byte[]{(byte) 5, (byte) 6};
        Byte[] byteArray4 = new Byte[]{(byte) 7, (byte) 8};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", byteArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", byteArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", byteArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", byteArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(byteArray1, byteArray3, byteArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(v1, v3, v4)));
    }

    @Test
    public void testBulkWithin_byteArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsByteArrayValues());
        byte[] byteArray1 = new byte[]{(byte) 1, (byte) 2};
        byte[] byteArray2 = new byte[]{(byte) 3, (byte) 4};
        byte[] byteArray3 = new byte[]{(byte) 5, (byte) 6};
        byte[] byteArray4 = new byte[]{(byte) 7, (byte) 8};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", byteArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", byteArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", byteArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", byteArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(byteArray1, byteArray3, byteArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(v1, v3, v4)));
    }

    @Test
    public void testBulkWithinShort() {
        this.sqlgGraph.addVertex(T.label, "A", "name", (short)1);
        this.sqlgGraph.addVertex(T.label, "A", "name", (short)2);
        this.sqlgGraph.addVertex(T.label, "A", "name", (short)3);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within((short)1, (short)2, (short)3)).toList();
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testBulkWithin_shortArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsShortArrayValues());
        short[] shortArray1 = new short[]{(short) 1, (short) 2, (short) 3};
        short[] shortArray2 = new short[]{(short) 4, (short) 5, (short) 6};
        short[] shortArray3 = new short[]{(short) 7, (short) 8, (short) 9};
        short[] shortArray4 = new short[]{(short) 10, (short) 11, (short) 12};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", shortArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", shortArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", shortArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", shortArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(shortArray1, shortArray3, shortArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithin_ShortArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsShortArrayValues());
        Short[] shortArray1 = new Short[]{(short) 1, (short) 2, (short) 3};
        Short[] shortArray2 = new Short[]{(short) 4, (short) 5, (short) 6};
        Short[] shortArray3 = new Short[]{(short) 7, (short) 8, (short) 9};
        Short[] shortArray4 = new Short[]{(short) 10, (short) 11, (short) 12};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", shortArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", shortArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", shortArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", shortArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(shortArray1, shortArray3, shortArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithinInteger() {
        this.sqlgGraph.addVertex(T.label, "A", "name", 1);
        this.sqlgGraph.addVertex(T.label, "A", "name", 2);
        this.sqlgGraph.addVertex(T.label, "A", "name", 3);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(1, 2, 3)).toList();
        Assert.assertEquals(3, vertices.size());
    }


    @Test
    public void testBulkWithin_intArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
        int[] intArray1 = new int[]{1, 2, 3};
        int[] intArray2 = new int[]{4, 5, 6};
        int[] intArray3 = new int[]{7, 8, 9};
        int[] intArray4 = new int[]{10, 11, 12};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", intArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", intArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", intArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", intArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(intArray1, intArray3, intArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithin_IntegerArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
        Integer[] intArray1 = new Integer[]{1, 2, 3};
        Integer[] intArray2 = new Integer[]{4, 5, 6};
        Integer[] intArray3 = new Integer[]{7, 8, 9};
        Integer[] intArray4 = new Integer[]{10, 11, 12};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", intArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", intArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", intArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", intArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(intArray1, intArray3, intArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithinLong() {
        this.sqlgGraph.addVertex(T.label, "A", "name", 1L);
        this.sqlgGraph.addVertex(T.label, "A", "name", 2L);
        this.sqlgGraph.addVertex(T.label, "A", "name", 3L);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(1L, 2L, 3L)).toList();
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testBulkWithin_longArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLongArrayValues());
        long[] longArray1 = new long[]{1L, 2L, 3L};
        long[] longArray2 = new long[]{4L, 5L, 6L};
        long[] longArray3 = new long[]{7L, 8L, 9L};
        long[] longArray4 = new long[]{10L, 11L, 12L};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", longArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", longArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", longArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", longArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(longArray1, longArray3, longArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithin_LongArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLongArrayValues());
        Long[] longArray1 = new Long[]{1L, 2L, 3L};
        Long[] longArray2 = new Long[]{4L, 5L, 6L};
        Long[] longArray3 = new Long[]{7L, 8L, 9L};
        Long[] longArray4 = new Long[]{10L, 11L, 12L};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", longArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", longArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", longArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", longArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(longArray1, longArray3, longArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithinFloat() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        this.sqlgGraph.addVertex(T.label, "A", "name", 1.1F);
        this.sqlgGraph.addVertex(T.label, "A", "name", 2.2F);
        this.sqlgGraph.addVertex(T.label, "A", "name", 3.3F);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(1.1F, 2.2F, 3.3F)).toList();
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testBulkWithin_floatArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatArrayValues());
        float[] floatArray1 = new float[]{1F, 2F, 3F};
        float[] floatArray2 = new float[]{4F, 5F, 6F};
        float[] floatArray3 = new float[]{7F, 8F, 9F};
        float[] floatArray4 = new float[]{10F, 11F, 12F};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", floatArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", floatArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", floatArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", floatArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(floatArray1, floatArray3, floatArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithin_FloatArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatArrayValues());
        Float[] floatArray1 = new Float[]{1F, 2F, 3F};
        Float[] floatArray2 = new Float[]{4F, 5F, 6F};
        Float[] floatArray3 = new Float[]{7F, 8F, 9F};
        Float[] floatArray4 = new Float[]{10F, 11F, 12F};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", floatArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", floatArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", floatArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", floatArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(floatArray1, floatArray3, floatArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithinDouble() {
        this.sqlgGraph.addVertex(T.label, "A", "name", 1.1D);
        this.sqlgGraph.addVertex(T.label, "A", "name", 2.2D);
        this.sqlgGraph.addVertex(T.label, "A", "name", 3.3D);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(1.1D, 2.2D, 3.3D)).toList();
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testBulkWithin_doubleArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDoubleArrayValues());
        double[] doubleArray1 = new double[]{1.1D, 2.2D, 3.3D};
        double[] doubleArray2 = new double[]{4.4D, 5.5D, 6.6D};
        double[] doubleArray3 = new double[]{7.7D, 8.8D, 9.9D};
        double[] doubleArray4 = new double[]{10.10D, 11.11D, 12.12D};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", doubleArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", doubleArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", doubleArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", doubleArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(doubleArray1, doubleArray3, doubleArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithin_DoubleArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDoubleArrayValues());
        Double[] doubleArray1 = new Double[]{1.1D, 2.2D, 3.3D};
        Double[] doubleArray2 = new Double[]{4.4D, 5.5D, 6.6D};
        Double[] doubleArray3 = new Double[]{7.7D, 8.8D, 9.9D};
        Double[] doubleArray4 = new Double[]{10.10D, 11.11D, 12.12D};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", doubleArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", doubleArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", doubleArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", doubleArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(doubleArray1, doubleArray3, doubleArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithinLocalDate() {
        this.sqlgGraph.addVertex(T.label, "A", "name", LocalDate.now());
        this.sqlgGraph.addVertex(T.label, "A", "name", LocalDate.now().minusDays(1));
        this.sqlgGraph.addVertex(T.label, "A", "name", LocalDate.now().minusDays(2));
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(LocalDate.now(), LocalDate.now().minusDays(1), LocalDate.now().minusDays(2))).toList();
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testBulkWithin_LocalDateArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalDateArrayValues());
        LocalDate[] localDateArray1 = new LocalDate[]{LocalDate.now().minusDays(1), LocalDate.now().minusDays(2), LocalDate.now().minusDays(3)};
        LocalDate[] localDateArray2 = new LocalDate[]{LocalDate.now().minusDays(4), LocalDate.now().minusDays(5), LocalDate.now().minusDays(6)};
        LocalDate[] localDateArray3 = new LocalDate[]{LocalDate.now().minusDays(7), LocalDate.now().minusDays(8), LocalDate.now().minusDays(9)};
        LocalDate[] localDateArray4 = new LocalDate[]{LocalDate.now().minusDays(10), LocalDate.now().minusDays(11), LocalDate.now().minusDays(12)};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", localDateArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", localDateArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", localDateArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", localDateArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(localDateArray1, localDateArray3, localDateArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithinLocalDateTime() {
        LocalDateTime localDateTime1 = LocalDateTime.now();
        LocalDateTime localDateTime2 = LocalDateTime.now().minusDays(1);
        LocalDateTime localDateTime3 = LocalDateTime.now().minusDays(2);
        if (isHsqldb() || isMariaDb()) {
            localDateTime1 = localDateTime1.truncatedTo(ChronoUnit.MILLIS);
            localDateTime2 = localDateTime2.truncatedTo(ChronoUnit.MILLIS);
            localDateTime3 = localDateTime3.truncatedTo(ChronoUnit.MILLIS);
        }
        this.sqlgGraph.addVertex(T.label, "A", "name", localDateTime1);
        this.sqlgGraph.addVertex(T.label, "A", "name", localDateTime2);
        this.sqlgGraph.addVertex(T.label, "A", "name", localDateTime3);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(localDateTime1, localDateTime2, localDateTime3)).toList();
        Assert.assertEquals(3, vertices.size());
    }

    //Hsqldb fails, looks like Hsqldb bug
    //TODO https://sourceforge.net/p/hsqldb/bugs/1495/
    @Test
    public void testBulkWithin_LocalDateTimeArray() {
        Assume.assumeFalse(this.sqlgGraph.getSqlDialect().isHsqldb());
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalDateTimeArrayValues());
        LocalDateTime[] localDateTimeArray1 = new LocalDateTime[]{LocalDateTime.now().minusDays(1), LocalDateTime.now().minusDays(2), LocalDateTime.now().minusDays(3)};
        LocalDateTime[] localDateTimeArray2 = new LocalDateTime[]{LocalDateTime.now().minusDays(4), LocalDateTime.now().minusDays(5), LocalDateTime.now().minusDays(6)};
        LocalDateTime[] localDateTimeArray3 = new LocalDateTime[]{LocalDateTime.now().minusDays(7), LocalDateTime.now().minusDays(8), LocalDateTime.now().minusDays(9)};
        LocalDateTime[] localDateTimeArray4 = new LocalDateTime[]{LocalDateTime.now().minusDays(10), LocalDateTime.now().minusDays(11), LocalDateTime.now().minusDays(12)};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", localDateTimeArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", localDateTimeArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", localDateTimeArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", localDateTimeArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(localDateTimeArray1, localDateTimeArray3, localDateTimeArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithinLocalTime() {
        LocalTime localTime1 = LocalTime.now();
        LocalTime localTime2 = LocalTime.now().minusHours(1);
        LocalTime localTime3 = LocalTime.now().minusHours(2);
        LOGGER.debug(localTime1.toString());
        LOGGER.debug(localTime2.toString());
        LOGGER.debug(localTime3.toString());
        LOGGER.debug(Calendar.getInstance().getTimeZone().toString());
        this.sqlgGraph.addVertex(T.label, "A", "name", localTime1);
        this.sqlgGraph.addVertex(T.label, "A", "name", localTime2);
        this.sqlgGraph.addVertex(T.label, "A", "name", localTime3);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(localTime1, localTime2, localTime3)).toList();
        Assert.assertEquals(3, vertices.size());
    }

    //TODO https://sourceforge.net/p/hsqldb/bugs/1495/
    @Test
    public void testBulkWithin_LocalTimeArray() {
        Assume.assumeFalse(this.sqlgGraph.getSqlDialect().isHsqldb());
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalTimeArrayValues());
        LocalTime[] localTimeArray1 = new LocalTime[]{LocalTime.now().minusHours(1), LocalTime.now().minusHours(2), LocalTime.now().minusHours(3)};
        LocalTime[] localTimeArray2 = new LocalTime[]{LocalTime.now().minusHours(4), LocalTime.now().minusHours(5), LocalTime.now().minusHours(6)};
        LocalTime[] localTimeArray3 = new LocalTime[]{LocalTime.now().minusHours(7), LocalTime.now().minusHours(8), LocalTime.now().minusHours(9)};
        LocalTime[] localTimeArray4 = new LocalTime[]{LocalTime.now().minusHours(10), LocalTime.now().minusHours(11), LocalTime.now().minusHours(12)};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", localTimeArray1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", localTimeArray2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", localTimeArray3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", localTimeArray4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(localTimeArray1, localTimeArray3, localTimeArray4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithinJson() {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode json1 = objectMapper.createObjectNode();
        json1.put("test", "aaa");
        ObjectNode json2 = objectMapper.createObjectNode();
        json2.put("test", "bbb");
        ObjectNode json3 = objectMapper.createObjectNode();
        json3.put("test", "ccc");
        ObjectNode json4 = objectMapper.createObjectNode();
        json4.put("test", "ddd");
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", json1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", json2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", json3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", json4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(json1, json3, json4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    @Test
    public void testBulkWithinJsonArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsJsonArrayValues());
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode json1_1 = objectMapper.createObjectNode();
        json1_1.put("test", "aaa");
        ObjectNode json1_2 = objectMapper.createObjectNode();
        json1_2.put("test", "bbb");
        ObjectNode json1_3 = objectMapper.createObjectNode();
        json1_3.put("test", "ccc");
        ObjectNode json1_4 = objectMapper.createObjectNode();
        json1_4.put("test", "ddd");

        ObjectNode[] jsons1 = new ObjectNode[]{json1_1, json1_2, json1_3, json1_4};

        ObjectNode json2_1 = objectMapper.createObjectNode();
        json2_1.put("test", "eee");
        ObjectNode json2_2 = objectMapper.createObjectNode();
        json2_2.put("test", "fff");
        ObjectNode json2_3 = objectMapper.createObjectNode();
        json2_3.put("test", "ggg");
        ObjectNode json2_4 = objectMapper.createObjectNode();
        json2_4.put("test", "hhh");

        ObjectNode[] jsons2 = new ObjectNode[]{json2_1, json2_2, json2_3, json2_4};

        ObjectNode json3_1 = objectMapper.createObjectNode();
        json3_1.put("test", "iii");
        ObjectNode json3_2 = objectMapper.createObjectNode();
        json3_2.put("test", "jjj");
        ObjectNode json3_3 = objectMapper.createObjectNode();
        json3_3.put("test", "kkk");
        ObjectNode json3_4 = objectMapper.createObjectNode();
        json3_4.put("test", "lll");

        ObjectNode[] jsons3 = new ObjectNode[]{json3_1, json3_2, json3_3, json3_4};

        ObjectNode json4_1 = objectMapper.createObjectNode();
        json4_1.put("test", "mmm");
        ObjectNode json4_2 = objectMapper.createObjectNode();
        json4_2.put("test", "nnn");
        ObjectNode json4_3 = objectMapper.createObjectNode();
        json4_3.put("test", "ooo");
        ObjectNode json4_4 = objectMapper.createObjectNode();
        json4_4.put("test", "ppp");

        ObjectNode[] jsons4 = new ObjectNode[]{json4_1, json4_2, json4_3, json4_4};

        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", jsons1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", jsons2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", jsons3);
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", jsons4);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within(jsons1, jsons3, jsons4)).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(Arrays.asList(v1, v3, v4).containsAll(vertices));
    }

    //TODO ZonedDateTime, Period, Duration

    @Test
    public void testBulkWithin() throws InterruptedException {
        if (this.sqlgGraph.getSqlDialect().supportsBatchMode()) {
            this.sqlgGraph.tx().normalBatchModeOn();
        }
        Vertex god = this.sqlgGraph.addVertex(T.label, "God");
        List<String> uuids = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String uuid = UUID.randomUUID().toString();
            uuids.add(uuid);
            Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "idNumber", uuid);
            god.addEdge("creator", person);
        }
        this.sqlgGraph.tx().commit();
        testBulkWithin_assert(this.sqlgGraph, uuids);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBulkWithin_assert(this.sqlgGraph1, uuids);
        }
    }

    private void testBulkWithin_assert(SqlgGraph sqlgGraph, List<String> uuids) {
        List<Vertex> persons = sqlgGraph.traversal().V().hasLabel("God").out().has("idNumber", P.within(uuids.subList(0, 2).toArray())).toList();
        Assert.assertEquals(2, persons.size());
        persons = sqlgGraph.traversal().V().hasLabel("God").out().has("idNumber", P.within(uuids.toArray())).toList();
        Assert.assertEquals(100, persons.size());
    }

    @Test
    public void testBulkWithinMultipleHasContainers() throws InterruptedException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        if (this.sqlgGraph.getSqlDialect().supportsBatchMode()) {
            this.sqlgGraph.tx().normalBatchModeOn();
        }
        Vertex god = this.sqlgGraph.addVertex(T.label, "God");
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "idNumber", 1, "name", "pete");
        god.addEdge("creator", person1);
        Vertex person2 = this.sqlgGraph.addVertex(T.label, "Person", "idNumber", 2, "name", "pete");
        god.addEdge("creator", person2);
        Vertex person3 = this.sqlgGraph.addVertex(T.label, "Person", "idNumber", 3, "name", "john");
        god.addEdge("creator", person3);
        Vertex person4 = this.sqlgGraph.addVertex(T.label, "Person", "idNumber", 4, "name", "pete");
        god.addEdge("creator", person4);
        Vertex person5 = this.sqlgGraph.addVertex(T.label, "Person", "idNumber", 5, "name", "pete");
        god.addEdge("creator", person5);
        Vertex person6 = this.sqlgGraph.addVertex(T.label, "Person", "idNumber", 6, "name", "pete");
        god.addEdge("creator", person6);
        Vertex person7 = this.sqlgGraph.addVertex(T.label, "Person", "idNumber", 7, "name", "pete");
        god.addEdge("creator", person7);
        Vertex person8 = this.sqlgGraph.addVertex(T.label, "Person", "idNumber", 8, "name", "pete");
        god.addEdge("creator", person8);
        Vertex person9 = this.sqlgGraph.addVertex(T.label, "Person", "idNumber", 9, "name", "pete");
        god.addEdge("creator", person9);
        Vertex person10 = this.sqlgGraph.addVertex(T.label, "Person", "idNumber", 10, "name", "pete");
        god.addEdge("creator", person10);

        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        testBulkWithinMultipleHasContrainers_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBulkWithinMultipleHasContrainers_assert(this.sqlgGraph);
        }
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }

    private void testBulkWithinMultipleHasContrainers_assert(SqlgGraph sqlgGraph) {
        List<Vertex> persons = sqlgGraph.traversal().V()
                .hasLabel("God")
                .out()
                .has("name", "pete")
                .has("idNumber", P.within(1,2,3))
                .toList();
        Assert.assertEquals(2, persons.size());
    }

    @Test
    public void testBulkWithinVertexCompileStep() throws InterruptedException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        if (this.sqlgGraph.getSqlDialect().supportsBatchMode()) {
            this.sqlgGraph.tx().normalBatchModeOn();
        }
        Vertex god = this.sqlgGraph.addVertex(T.label, "God");
        List<String> uuids = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String uuid = UUID.randomUUID().toString();
            uuids.add(uuid);
            Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "idNumber", uuid);
            god.addEdge("creator", person);
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        testBulkWithinVertexCompileStep_assert(this.sqlgGraph, god, uuids);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBulkWithinVertexCompileStep_assert(this.sqlgGraph1, god, uuids);
        }
        stopWatch.stop();
        System.out.println(stopWatch.toString());

    }

    private void testBulkWithinVertexCompileStep_assert(SqlgGraph sqlgGraph, Vertex god, List<String> uuids) {
        List<Vertex> persons = sqlgGraph.traversal().V(god.id()).out().has("idNumber", P.within(uuids.subList(0, 2).toArray())).toList();
        Assert.assertEquals(2, persons.size());
        persons = sqlgGraph.traversal().V().hasLabel("God").out().has("idNumber", P.within(uuids.toArray())).toList();
        Assert.assertEquals(100, persons.size());
    }

    @Test
    public void testBulkWithinWithPercentageInJoinProperties() throws InterruptedException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        if (this.sqlgGraph.getSqlDialect().supportsBatchMode()) {
            this.sqlgGraph.tx().normalBatchModeOn();
        }
        Vertex god = this.sqlgGraph.addVertex(T.label, "God");
        List<String> uuids = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String uuid = UUID.randomUUID().toString();
            uuids.add("\"BLRNC5->CXC4030052~%%%~FAJ1211373~%%%~2015-07-19~%%%~9999-12-31~%%%~Enabled~%%%~Licensed~%%%~Improved~%%%~compressed~%%%~mode~%%%~handling.~%%%~Restricted:~%%%~\"\"Partial.~%%%~Feature~%%%~is~%%%~restricted~%%%~in~%%%~RNC~%%%~W12B~%%%~SW.~%%%~RNC~%%%~W13.0.1.1~%%%~or~%%%~later~%%%~SW~%%%~is~%%%~required~%%%~in~%%%~order~%%%~to~%%%~run~%%%~this~%%%~feature.~%%%~For~%%%~RBS~%%%~W12.1.2.2/~%%%~W13.0.0.0~%%%~or~%%%~later~%%%~is~%%%~required.~%%%~OSS-RC~%%%~12.2~%%%~or~%%%~later~%%%~is~%%%~required.\"\".~%%%~GA:~%%%~W13A\"" + uuid);
            Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "idNumber", "\"BLRNC5->CXC4030052~%%%~FAJ1211373~%%%~2015-07-19~%%%~9999-12-31~%%%~Enabled~%%%~Licensed~%%%~Improved~%%%~compressed~%%%~mode~%%%~handling.~%%%~Restricted:~%%%~\"\"Partial.~%%%~Feature~%%%~is~%%%~restricted~%%%~in~%%%~RNC~%%%~W12B~%%%~SW.~%%%~RNC~%%%~W13.0.1.1~%%%~or~%%%~later~%%%~SW~%%%~is~%%%~required~%%%~in~%%%~order~%%%~to~%%%~run~%%%~this~%%%~feature.~%%%~For~%%%~RBS~%%%~W12.1.2.2/~%%%~W13.0.0.0~%%%~or~%%%~later~%%%~is~%%%~required.~%%%~OSS-RC~%%%~12.2~%%%~or~%%%~later~%%%~is~%%%~required.\"\".~%%%~GA:~%%%~W13A\"" + uuid);
            god.addEdge("creator", person);
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        testBulkWithinWithPercentageInJoinProperties_assert(this.sqlgGraph, uuids);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBulkWithinWithPercentageInJoinProperties_assert(this.sqlgGraph1, uuids);
        }
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }

    private void testBulkWithinWithPercentageInJoinProperties_assert(SqlgGraph sqlgGraph, List<String> uuids) {
        List<Vertex> persons = sqlgGraph.traversal().V().hasLabel("God").out().has("idNumber", P.within(uuids.subList(0, 2).toArray())).toList();
        Assert.assertEquals(2, persons.size());
        persons = this.sqlgGraph.traversal().V().hasLabel("God").out().has("idNumber", P.within(uuids.toArray())).toList();
        Assert.assertEquals(100, persons.size());
    }
}
