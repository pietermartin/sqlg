package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2015/12/14
 * Time: 8:56 PM
 */
public class TestBatchNormalUpdate extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }
    
    @Test
    public void testUpdateWithCommaInValues() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a1", "name2", "a11", "name3", "a111");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a2", "name2", "a22", "name3", "a222");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a3", "name2", "a33", "name3", "a333");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        a1.property("name1", "a1,a2");
        a2.property("name1", "a1,a2");
        this.sqlgGraph.tx().commit();


    }

//    @Test
//    public void testUpdateWithNullStringValuesAlreadyPresent() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a1", "name2", "a11", "name3", "a111");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a2", "name2", "a22", "name3", "a222");
//        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a3", "name2", "a33", "name3", "a333");
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        a1.property("name1", "aa1");
//        a2.property("name2", "'aa2'");
//        a3.property("name3", "$token$");
//        this.sqlgGraph.tx().commit();
//        testUpdateWithNullStringValuesAlreadyPresent_assert(this.sqlgGraph, a1, a2, a3);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testUpdateWithNullStringValuesAlreadyPresent_assert(this.sqlgGraph, a1, a2, a3);
//        }
//    }
//
//    private void testUpdateWithNullStringValuesAlreadyPresent_assert(SqlgGraph sqlgGraph, Vertex a1, Vertex a2, Vertex a3) {
//        a1 = sqlgGraph.traversal().V(a1.id()).next();
//        a2 = sqlgGraph.traversal().V(a2.id()).next();
//        a3 = sqlgGraph.traversal().V(a3.id()).next();
//
//        Assert.assertEquals("aa1", a1.value("name1"));
//        Assert.assertEquals("a11", a1.value("name2"));
//        Assert.assertEquals("a111", a1.value("name3"));
//
//        Assert.assertEquals("a2", a2.value("name1"));
//        Assert.assertEquals("'aa2'", a2.value("name2"));
//        Assert.assertEquals("a222", a2.value("name3"));
//
//        Assert.assertEquals("a3", a3.value("name1"));
//        Assert.assertEquals("a33", a3.value("name2"));
//        Assert.assertEquals("$token$", a3.value("name3"));
//    }
//
//    @Test
//    public void testUpdateWithNullStringValuesNotAlreadyPresent() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name2", "a2");
//        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name3", "a3");
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        a1.property("name1", "aa1");
//        a2.property("name2", "aa2");
//        a3.property("name3", "aa3");
//        this.sqlgGraph.tx().commit();
//        testUpdateWithNullStringValuesNotAlreadyPresent_assert(this.sqlgGraph, a1, a2, a3);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testUpdateWithNullStringValuesNotAlreadyPresent_assert(this.sqlgGraph1, a1, a2, a3);
//        }
//    }
//
//    private void testUpdateWithNullStringValuesNotAlreadyPresent_assert(SqlgGraph sqlgGraph, Vertex a1, Vertex a2, Vertex a3) {
//        Assert.assertFalse(sqlgGraph.traversal().V(a1.id()).next().property("name2").isPresent());
//        Assert.assertFalse(sqlgGraph.traversal().V(a1.id()).next().property("name3").isPresent());
//        Assert.assertFalse(sqlgGraph.traversal().V(a2.id()).next().property("name1").isPresent());
//        Assert.assertFalse(sqlgGraph.traversal().V(a2.id()).next().property("name3").isPresent());
//        Assert.assertFalse(sqlgGraph.traversal().V(a3.id()).next().property("name1").isPresent());
//        Assert.assertFalse(sqlgGraph.traversal().V(a3.id()).next().property("name2").isPresent());
//    }
//
//    @Test
//    public void testUpdateWithNullInteger() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name1", 1);
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name2", 2);
//        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name3", 3);
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        a1.property("name1", 11);
//        a2.property("name2", 22);
//        a3.property("name3", 33);
//        this.sqlgGraph.tx().commit();
//        testUpdateWithNullInteger_assert(this.sqlgGraph, a1, a2, a3);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testUpdateWithNullInteger_assert(this.sqlgGraph1, a1, a2, a3);
//        }
//    }
//
//    private void testUpdateWithNullInteger_assert(SqlgGraph sqlgGraph, Vertex a1, Vertex a2, Vertex a3) {
//        Assert.assertFalse(sqlgGraph.traversal().V(a1.id()).next().property("name2").isPresent());
//        Assert.assertFalse(sqlgGraph.traversal().V(a1.id()).next().property("name3").isPresent());
//        Assert.assertFalse(sqlgGraph.traversal().V(a2.id()).next().property("name1").isPresent());
//        Assert.assertFalse(sqlgGraph.traversal().V(a2.id()).next().property("name3").isPresent());
//        Assert.assertFalse(sqlgGraph.traversal().V(a3.id()).next().property("name1").isPresent());
//        Assert.assertFalse(sqlgGraph.traversal().V(a3.id()).next().property("name2").isPresent());
//    }
//
//    @Test
//    public void testUpdateWithoutDots() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        for (int i = 0; i < 4; i++) {
//            this.sqlgGraph.addVertex(T.label, "GTRX", "test1", "a1", "test2", "a2", "cm_uid", "cm_uid_" + i);
//        }
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        GraphTraversalSource gts = this.sqlgGraph.traversal();
//        Vertex cmUid0 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next();
//        cmUid0.property("test1", "b1");
//
//        Vertex cmUid1 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next();
//        cmUid1.property("test2", "b1");
//
//        Vertex cmUid2 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next();
//        cmUid2.property("test1", "b1");
//
//        Vertex cmUid3 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next();
//        cmUid3.property("test2", "b1");
//        this.sqlgGraph.tx().commit();
//
//        testUpdateWithoutDots_assert(gts);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            gts = this.sqlgGraph1.traversal();
//            testUpdateWithoutDots_assert(gts);
//        }
//    }
//
//    private void testUpdateWithoutDots_assert(GraphTraversalSource gts) {
//        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next().value("test1"));
//        Assert.assertEquals("a2", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next().value("test2"));
//
//        Assert.assertEquals("a1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next().value("test1"));
//        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next().value("test2"));
//
//        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next().value("test1"));
//        Assert.assertEquals("a2", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next().value("test2"));
//
//        Assert.assertEquals("a1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next().value("test1"));
//        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next().value("test2"));
//    }
//
//    @Test
//    public void testUpdateWithDots() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        for (int i = 0; i < 4; i++) {
//            this.sqlgGraph.addVertex(T.label, "GTRX", "test1", "a1", "test.2", "a2", "cm_uid", "cm_uid_" + i);
//        }
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        GraphTraversalSource gts = this.sqlgGraph.traversal();
//        Vertex cmUid0 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next();
//        cmUid0.property("test1", "b1");
//
//        Vertex cmUid1 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next();
//        cmUid1.property("test.2", "b1");
//
//        Vertex cmUid2 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next();
//        cmUid2.property("test1", "b1");
//
//        Vertex cmUid3 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next();
//        cmUid3.property("test.2", "b1");
//        this.sqlgGraph.tx().commit();
//
//        testUpdateWithDots_assert(gts);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            gts = this.sqlgGraph1.traversal();
//            testUpdateWithDots_assert(gts);
//        }
//    }
//
//    private void testUpdateWithDots_assert(GraphTraversalSource gts) {
//        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next().value("test1"));
//        Assert.assertEquals("a2", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next().value("test.2"));
//
//        Assert.assertEquals("a1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next().value("test1"));
//        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next().value("test.2"));
//
//        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next().value("test1"));
//        Assert.assertEquals("a2", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next().value("test.2"));
//
//        Assert.assertEquals("a1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next().value("test1"));
//        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next().value("test.2"));
//    }
//
//    @Test
//    public void testUpdateArrays() throws InterruptedException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatArrayValues());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v = this.sqlgGraph.addVertex(T.label, "Person",
//                "names", new String[]{"A", "B"},
//                "integers", new int[]{1, 2},
//                "booleans", new boolean[]{true, true},
//                "doubles", new double[]{1D, 2D},
//                "longs", new long[]{1L, 2L},
//                "floats", new float[]{1F, 2F},
//                "shorts", new short[]{1, 2},
//                "bytes", new byte[]{1, 2}
//        );
//        this.sqlgGraph.tx().commit();
//        Assert.assertTrue(this.sqlgGraph.traversal().V().hasLabel("Person").hasNext());
//        Assert.assertTrue(this.sqlgGraph.traversal().V().hasLabel("Person").next().property("names").isPresent());
//        Assert.assertArrayEquals(new String[]{"A", "B"}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<String[]>value("names"));
//        Assert.assertArrayEquals(new byte[]{1, 2}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<byte[]>value("bytes"));
//        this.sqlgGraph.tx().normalBatchModeOn();
//        v.property("names", new String[]{"C", "D"});
//        v.property("integers", new int[]{3, 4});
//        v.property("booleans", new boolean[]{false, false});
//        v.property("doubles", new double[]{3D, 4D});
//        v.property("longs", new long[]{3L, 4L});
//        v.property("floats", new float[]{3F, 4F});
//        v.property("shorts", new short[]{3, 4});
//        v.property("bytes", new byte[]{3, 4});
//        this.sqlgGraph.tx().commit();
//        testUpdateArrays_assert(this.sqlgGraph);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testUpdateArrays_assert(this.sqlgGraph1);
//        }
//    }
//
//    private void testUpdateArrays_assert(SqlgGraph sqlgGraph) {
//        Assert.assertArrayEquals(new String[]{"C", "D"}, sqlgGraph.traversal().V().hasLabel("Person").next().<String[]>value("names"));
//        Assert.assertArrayEquals(new int[]{3, 4}, sqlgGraph.traversal().V().hasLabel("Person").next().<int[]>value("integers"));
//        Assert.assertArrayEquals(new boolean[]{false, false}, sqlgGraph.traversal().V().hasLabel("Person").next().<boolean[]>value("booleans"));
//        Assert.assertArrayEquals(new double[]{3D, 4D}, sqlgGraph.traversal().V().hasLabel("Person").next().<double[]>value("doubles"), 0d);
//        Assert.assertArrayEquals(new long[]{3L, 4L}, sqlgGraph.traversal().V().hasLabel("Person").next().<long[]>value("longs"));
//        Assert.assertArrayEquals(new float[]{3F, 4F}, sqlgGraph.traversal().V().hasLabel("Person").next().<float[]>value("floats"), 0f);
//        Assert.assertArrayEquals(new short[]{3, 4}, sqlgGraph.traversal().V().hasLabel("Person").next().<short[]>value("shorts"));
//        Assert.assertArrayEquals(new byte[]{3, 4}, sqlgGraph.traversal().V().hasLabel("Person").next().<byte[]>value("bytes"));
//    }
//
//    @Test
//    public void testUpdateArraysNoFloats() throws InterruptedException {
//        Assume.assumeTrue(!this.sqlgGraph.getSqlDialect().isMariaDb() && !this.sqlgGraph.getSqlDialect().isMssqlServer());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v = this.sqlgGraph.addVertex(T.label, "Person",
//                "names", new String[]{"A", "B"},
//                "integers", new int[]{1, 2},
//                "booleans", new boolean[]{true, true},
//                "doubles", new double[]{1D, 2D},
//                "longs", new long[]{1L, 2L},
//                "shorts", new short[]{1, 2},
//                "bytes", new byte[]{1, 2}
//        );
//        this.sqlgGraph.tx().commit();
//        Assert.assertTrue(this.sqlgGraph.traversal().V().hasLabel("Person").hasNext());
//        Assert.assertTrue(this.sqlgGraph.traversal().V().hasLabel("Person").next().property("names").isPresent());
//        Assert.assertArrayEquals(new String[]{"A", "B"}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<String[]>value("names"));
//        Assert.assertArrayEquals(new byte[]{1, 2}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<byte[]>value("bytes"));
//        this.sqlgGraph.tx().normalBatchModeOn();
//        v.property("names", new String[]{"C", "D"});
//        v.property("integers", new int[]{3, 4});
//        v.property("booleans", new boolean[]{false, false});
//        v.property("doubles", new double[]{3D, 4D});
//        v.property("longs", new long[]{3L, 4L});
//        v.property("shorts", new short[]{3, 4});
//        v.property("bytes", new byte[]{3, 4});
//        this.sqlgGraph.tx().commit();
//        testUpdateArraysNoFloats_assert(this.sqlgGraph);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testUpdateArraysNoFloats_assert(this.sqlgGraph1);
//        }
//    }
//
//    private void testUpdateArraysNoFloats_assert(SqlgGraph sqlgGraph) {
//        Assert.assertArrayEquals(new String[]{"C", "D"}, sqlgGraph.traversal().V().hasLabel("Person").next().<String[]>value("names"));
//        Assert.assertArrayEquals(new int[]{3, 4}, sqlgGraph.traversal().V().hasLabel("Person").next().<int[]>value("integers"));
//        Assert.assertArrayEquals(new boolean[]{false, false}, sqlgGraph.traversal().V().hasLabel("Person").next().<boolean[]>value("booleans"));
//        Assert.assertArrayEquals(new double[]{3D, 4D}, sqlgGraph.traversal().V().hasLabel("Person").next().<double[]>value("doubles"), 0d);
//        Assert.assertArrayEquals(new long[]{3L, 4L}, sqlgGraph.traversal().V().hasLabel("Person").next().<long[]>value("longs"));
//        Assert.assertArrayEquals(new short[]{3, 4}, sqlgGraph.traversal().V().hasLabel("Person").next().<short[]>value("shorts"));
//        Assert.assertArrayEquals(new byte[]{3, 4}, sqlgGraph.traversal().V().hasLabel("Person").next().<byte[]>value("bytes"));
//    }
//
//    @Test
//    public void testStringUpdate() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", "halo");
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
//        v1.property("name", "halo3");
//        this.sqlgGraph.tx().commit();
//        testStringUpdate_assert(this.sqlgGraph, v1);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testStringUpdate_assert(this.sqlgGraph1, v1);
//        }
//    }
//
//    private void testStringUpdate_assert(SqlgGraph sqlgGraph, Vertex v1) {
//        v1 = sqlgGraph.traversal().V(v1.id()).next();
//        Assert.assertEquals("halo3", v1.value("name"));
//    }
//
//    @Test
//    public void testStringUpdateEdge() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A");
//        Edge e1 = v1.addEdge("test", v2, "name", "halo");
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
//        e1.property("name", "halo3");
//        this.sqlgGraph.tx().commit();
//        testStringUpdateEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testStringUpdateEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testStringUpdateEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        Assert.assertEquals("halo3", e1.value("name"));
//    }
//
//    @Test
//    public void testBooleanUpdate() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", Boolean.valueOf(true));
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
//        v1.property("name", Boolean.valueOf(false));
//        this.sqlgGraph.tx().commit();
//        testBooleanUpdate_assert(this.sqlgGraph, v1);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testBooleanUpdate_assert(this.sqlgGraph1, v1);
//        }
//    }
//
//    private void testBooleanUpdate_assert(SqlgGraph sqlgGraph, Vertex v1) {
//        v1 = sqlgGraph.traversal().V(v1.id()).next();
//        Assert.assertEquals(Boolean.valueOf(false), v1.value("name"));
//    }
//
//    @Test
//    public void testBooleanUpdateEdge() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A");
//        Edge e1 = v1.addEdge("test", v2, "name", Boolean.valueOf(true));
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
//        e1.property("name", Boolean.valueOf(false));
//        this.sqlgGraph.tx().commit();
//        testBooleanUpdateEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testBooleanUpdateEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testBooleanUpdateEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        Assert.assertEquals(Boolean.valueOf(false), e1.value("name"));
//        Assert.assertTrue(e1.value("name").getClass().equals(Boolean.class));
//    }
//
//    @Test
//    public void testShortUpdate() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", Short.valueOf("1"));
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
//        v1.property("name", Short.valueOf("2"));
//        this.sqlgGraph.tx().commit();
//        testShortUpdate_assert(this.sqlgGraph, v1);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testShortUpdate_assert(this.sqlgGraph1, v1);
//        }
//    }
//
//    private void testShortUpdate_assert(SqlgGraph sqlgGraph, Vertex v1) {
//        v1 = sqlgGraph.traversal().V(v1.id()).next();
//        Assert.assertEquals(Short.valueOf("2"), v1.value("name"));
//    }
//
//    @Test
//    public void testShortUpdateEdge() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A");
//        Edge e1 = v1.addEdge("test", v2, "name", Short.valueOf("1"));
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
//        e1.property("name", Short.valueOf("2"));
//        this.sqlgGraph.tx().commit();
//        testShortUpdateEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testShortUpdateEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testShortUpdateEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        Assert.assertEquals(Short.valueOf("2"), e1.value("name"));
//    }
//
//    @Test
//    public void testIntegerUpdate() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", Integer.valueOf("1"));
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
//        v1.property("name", Integer.valueOf("2"));
//        this.sqlgGraph.tx().commit();
//        testIntegerUpdate_assert(this.sqlgGraph, v1);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testIntegerUpdate_assert(this.sqlgGraph1, v1);
//        }
//    }
//
//    private void testIntegerUpdate_assert(SqlgGraph sqlgGraph, Vertex v1) {
//        v1 = sqlgGraph.traversal().V(v1.id()).next();
//        Assert.assertEquals(Integer.valueOf("2"), v1.value("name"));
//    }
//
//    @Test
//    public void testIntegerUpdateEdge() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A");
//        Edge e1 = v1.addEdge("test", v2, "name", Integer.valueOf("1"));
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
//        e1.property("name", Integer.valueOf("2"));
//        this.sqlgGraph.tx().commit();
//        testIntegerUpdateEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testIntegerUpdateEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testIntegerUpdateEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        Assert.assertEquals(Integer.valueOf("2"), e1.value("name"));
//    }
//
//    @Test
//    public void testLongUpdate() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", Long.valueOf("1"));
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
//        v1.property("name", Long.valueOf("2"));
//        this.sqlgGraph.tx().commit();
//        testLongUpdate_assert(this.sqlgGraph, v1);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testLongUpdate_assert(this.sqlgGraph1, v1);
//        }
//    }
//
//    private void testLongUpdate_assert(SqlgGraph sqlgGraph, Vertex v1) {
//        v1 = sqlgGraph.traversal().V(v1.id()).next();
//        Assert.assertEquals(Long.valueOf("2"), v1.value("name"));
//    }
//
//    @Test
//    public void testLongUpdateEdge() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A");
//        Edge e1 = v1.addEdge("test", v2, "name", Long.valueOf("1"));
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
//        e1.property("name", Long.valueOf("2"));
//        this.sqlgGraph.tx().commit();
//        testLongUpdateEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testLongUpdateEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testLongUpdateEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        Assert.assertEquals(Long.valueOf("2"), e1.value("name"));
//    }
//
//    @Test
//    public void testFloatUpdate() throws InterruptedException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", Float.valueOf("1.1"));
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
//        v1.property("name", Float.valueOf("2.2"));
//        this.sqlgGraph.tx().commit();
//        testFloatUpdate_assert(this.sqlgGraph, v1);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testFloatUpdate_assert(this.sqlgGraph1, v1);
//        }
//    }
//
//    private void testFloatUpdate_assert(SqlgGraph sqlgGraph, Vertex v1) {
//        v1 = sqlgGraph.traversal().V(v1.id()).next();
//        Assert.assertEquals(Float.valueOf("2.2"), v1.value("name"));
//    }
//
//    @Test
//    public void testFloatUpdateEdge() throws InterruptedException {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A");
//        Edge e1 = v1.addEdge("test", v2, "name", Float.valueOf("1.1"));
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
//        e1.property("name", Float.valueOf("2.2"));
//        this.sqlgGraph.tx().commit();
//        testFloatUpdateEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 != null) {
//            sleep(SLEEP_TIME);
//            testFloatUpdateEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testFloatUpdateEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        Assert.assertEquals(Float.valueOf("2.2"), e1.value("name"));
//    }
//
//    @Test
//    public void testDoubleUpdate() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", Double.valueOf("1.1"));
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
//        v1.property("name", Double.valueOf("2.2"));
//        this.sqlgGraph.tx().commit();
//        testDoubleUpdate_assert(this.sqlgGraph, v1);
//        if (this.sqlgGraph1 != null) {
//            Thread.sleep(SLEEP_TIME);
//            testDoubleUpdate_assert(this.sqlgGraph1, v1);
//        }
//    }
//
//    private void testDoubleUpdate_assert(SqlgGraph sqlgGraph, Vertex v1) {
//        v1 = sqlgGraph.traversal().V(v1.id()).next();
//        Assert.assertEquals(Double.valueOf("2.2"), v1.value("name"));
//    }
//
//    @Test
//    public void testDoubleUpdateEdge() throws InterruptedException {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A");
//        Edge e1 = v1.addEdge("test", v2, "name", Double.valueOf("1.1"));
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
//        e1.property("name", Double.valueOf("2.2"));
//        this.sqlgGraph.tx().commit();
//        testDoubleUpdateEdge_assert(this.sqlgGraph, e1);
//        if (this.sqlgGraph1 !=  null) {
//            Thread.sleep(SLEEP_TIME);
//            testDoubleUpdateEdge_assert(this.sqlgGraph1, e1);
//        }
//    }
//
//    private void testDoubleUpdateEdge_assert(SqlgGraph sqlgGraph, Edge e1) {
//        e1 = sqlgGraph.traversal().E(e1.id()).next();
//        Assert.assertEquals(Double.valueOf("2.2"), e1.value("name"));
//    }

}
