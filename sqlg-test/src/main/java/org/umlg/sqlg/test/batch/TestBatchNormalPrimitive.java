package org.umlg.sqlg.test.batch;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.lang.reflect.Array;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Date: 2016/05/22
 * Time: 9:13 AM
 */
public class TestBatchNormalPrimitive extends BaseTest {

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
    public void testString() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Vertex> vertexSet = new HashSet<>();
        Set<String> vertexNameSet = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String s = "name" + i;
            vertexSet.add(this.sqlgGraph.addVertex(T.label, "A", "name", s));
            vertexNameSet.add(s);
        }
        this.sqlgGraph.tx().commit();
        testString_assert(this.sqlgGraph, vertexSet, vertexNameSet);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testString_assert(this.sqlgGraph1, vertexSet, vertexNameSet);
        }
    }

    private void testString_assert(SqlgGraph sqlgGraph, Set<Vertex> vertexSet, Set<String> vertexNameSet) {
        Set<Vertex> vertices = sqlgGraph.traversal().V().toSet();
        Set<String> verticesName = sqlgGraph.traversal().V().<String>values("name").toSet();
        assertEquals(10, vertices.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(vertexSet, vertices));
        assertTrue(CollectionUtils.isEqualCollection(vertexNameSet, verticesName));
    }

    @Test
    public void testStringEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Edge> edgeSet = new HashSet<>();
        Set<String> edgeNameSet = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String s = "name" + i;
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            edgeSet.add(vertex1.addEdge("test", vertex2, "name", s));
            edgeNameSet.add(s);
        }
        this.sqlgGraph.tx().commit();
        testStringEdge_assert(this.sqlgGraph, edgeSet, edgeNameSet);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStringEdge_assert(this.sqlgGraph1, edgeSet, edgeNameSet);
        }
    }

    private void testStringEdge_assert(SqlgGraph sqlgGraph, Set<Edge> edgeSet, Set<String> edgeNameSet) {
        Set<Edge> edges = sqlgGraph.traversal().E().toSet();
        Set<String> edgesName = sqlgGraph.traversal().E().<String>values("name").toSet();
        assertEquals(10, edges.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(edgeSet, edges));
        assertTrue(CollectionUtils.isEqualCollection(edgeNameSet, edgesName));
    }

    @Test
    public void testBoolean() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Vertex> vertexSet = new HashSet<>();
        Set<Boolean> vertexNameSet = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            Boolean b = i % 2 == 0;
            vertexSet.add(this.sqlgGraph.addVertex(T.label, "A", "name", new Boolean(b)));
            vertexNameSet.add(new Boolean(b));
        }
        this.sqlgGraph.tx().commit();
        testBoolean_assert(this.sqlgGraph, vertexSet, vertexNameSet);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBoolean_assert(this.sqlgGraph1, vertexSet, vertexNameSet);
        }
    }

    private void testBoolean_assert(SqlgGraph sqlgGraph, Set<Vertex> vertexSet, Set<Boolean> vertexNameSet) {
        Set<Vertex> vertices = sqlgGraph.traversal().V().toSet();
        Set<Boolean> verticesName = sqlgGraph.traversal().V().<Boolean>values("name").toSet();
        assertEquals(10, vertices.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(vertexSet, vertices));
        assertTrue(CollectionUtils.isEqualCollection(vertexNameSet, verticesName));
    }

    @Test
    public void testBooleanEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Edge> edgeSet = new HashSet<>();
        Set<Boolean> edgeNameSet = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            Boolean b = i % 2 == 0;
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            edgeSet.add(vertex1.addEdge("test", vertex2, "name", new Boolean(b)));
            edgeNameSet.add(new Boolean(b));
        }
        this.sqlgGraph.tx().commit();
        testBooleanEdge_assert(this.sqlgGraph, edgeSet, edgeNameSet);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBooleanEdge_assert(this.sqlgGraph1, edgeSet, edgeNameSet);
        }
    }

    private void testBooleanEdge_assert(SqlgGraph sqlgGraph, Set<Edge> edgeSet, Set<Boolean> edgeNameSet) {
        Set<Edge> edges = sqlgGraph.traversal().E().toSet();
        Set<Boolean> edgesName = sqlgGraph.traversal().E().<Boolean>values("name").toSet();
        assertEquals(10, edges.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(edgeSet, edges));
        assertTrue(CollectionUtils.isEqualCollection(edgeNameSet, edgesName));
    }

    @Test
    public void testBooleanPrimitive() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        boolean[] vertexNameArray = new boolean[10];
        for (int i = 0; i < 10; i++) {
            boolean b = i % 2 == 0;
            this.sqlgGraph.addVertex(T.label, "A", "name", b);
            Array.set(vertexNameArray, i, b);
        }
        this.sqlgGraph.tx().commit();
        testBooleanPrimitive_assert(this.sqlgGraph, vertexNameArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBooleanPrimitive_assert(this.sqlgGraph1, vertexNameArray);
        }
    }

    private void testBooleanPrimitive_assert(SqlgGraph sqlgGraph, boolean[] vertexNameArray) {
        GraphTraversal<Vertex, Vertex> traversal = sqlgGraph.traversal().V();
        int count = 0;
        boolean[] vertexNameArrayToTest = new boolean[10];
        while (traversal.hasNext()) {
            Array.set(vertexNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(vertexNameArray, vertexNameArrayToTest);
    }

    @Test
    public void testBooleanPrimitiveEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        boolean[] edgeNameArray = new boolean[10];
        for (int i = 0; i < 10; i++) {
            boolean b = i % 2 == 0;
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            vertex1.addEdge("test", vertex2, "name", b);
            Array.set(edgeNameArray, i, b);
        }
        this.sqlgGraph.tx().commit();
        testBooleanPrimitiveEdge_assert(this.sqlgGraph, edgeNameArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBooleanPrimitiveEdge_assert(this.sqlgGraph1, edgeNameArray);
        }
    }

    private void testBooleanPrimitiveEdge_assert(SqlgGraph sqlgGraph, boolean[] edgeNameArray) {
        GraphTraversal<Edge, Edge> traversal = sqlgGraph.traversal().E();
        int count = 0;
        boolean[] edgeNameArrayToTest = new boolean[10];
        while (traversal.hasNext()) {
            Array.set(edgeNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(edgeNameArray, edgeNameArrayToTest);
    }

    @Test
    public void testInteger() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Vertex> vertexSet = new HashSet<>();
        Set<Integer> vertexNameSet = new HashSet<>();
        for (Integer i = 0; i < 10; i++) {
            vertexSet.add(this.sqlgGraph.addVertex(T.label, "A", "name", i));
            vertexNameSet.add(i);
        }
        this.sqlgGraph.tx().commit();
        testInteger_assert(this.sqlgGraph, vertexSet, vertexNameSet);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testInteger_assert(this.sqlgGraph1, vertexSet, vertexNameSet);
        }
    }

    private void testInteger_assert(SqlgGraph sqlgGraph, Set<Vertex> vertexSet, Set<Integer> vertexNameSet) {
        Set<Vertex> vertices = sqlgGraph.traversal().V().toSet();
        Set<Integer> verticesName = sqlgGraph.traversal().V().<Integer>values("name").toSet();
        assertEquals(10, vertices.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(vertexSet, vertices));
        assertTrue(CollectionUtils.isEqualCollection(vertexNameSet, verticesName));
    }

    @Test
    public void testIntegerEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Edge> edgeSet = new HashSet<>();
        Set<Integer> edgeNameSet = new HashSet<>();
        for (Integer i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            edgeSet.add(vertex1.addEdge("test", vertex2, "name", i));
            edgeNameSet.add(i);
        }
        this.sqlgGraph.tx().commit();
        testIntegerEdge_assert(this.sqlgGraph, edgeSet, edgeNameSet);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testIntegerEdge_assert(this.sqlgGraph1, edgeSet, edgeNameSet);
        }
    }

    private void testIntegerEdge_assert(SqlgGraph sqlgGraph, Set<Edge> edgeSet, Set<Integer> edgeNameSet) {
        Set<Edge> edges = sqlgGraph.traversal().E().toSet();
        Set<Integer> edgesName = sqlgGraph.traversal().E().<Integer>values("name").toSet();
        assertEquals(10, edges.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(edgeSet, edges));
        assertTrue(CollectionUtils.isEqualCollection(edgeNameSet, edgesName));
    }

    @Test
    public void testIntegerPrimitive() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        int[] vertexNameArray = new int[10];
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "name", i);
            Array.set(vertexNameArray, i, i);
        }
        this.sqlgGraph.tx().commit();
        testIntegerPrimitive_assert(this.sqlgGraph, vertexNameArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testIntegerPrimitive_assert(this.sqlgGraph1, vertexNameArray);
        }
    }

    private void testIntegerPrimitive_assert(SqlgGraph sqlgGraph, int[] vertexNameArray) {
        GraphTraversal<Vertex, Vertex> traversal = sqlgGraph.traversal().V();
        int count = 0;
        int[] vertexNameArrayToTest = new int[10];
        while (traversal.hasNext()) {
            Array.set(vertexNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(vertexNameArray, vertexNameArrayToTest);
    }

    @Test
    public void testIntegerPrimitiveEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        int[] edgeNameArray = new int[10];
        for (int i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            vertex1.addEdge("test", vertex2, "name", i);
            Array.set(edgeNameArray, i, i);
        }
        this.sqlgGraph.tx().commit();
        testIntegerPrimitiveEdge_assert(this.sqlgGraph, edgeNameArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testIntegerPrimitiveEdge_assert(this.sqlgGraph1, edgeNameArray);
        }
    }

    private void testIntegerPrimitiveEdge_assert(SqlgGraph sqlgGraph, int[] edgeNameArray) {
        GraphTraversal<Edge, Edge> traversal = sqlgGraph.traversal().E();
        int count = 0;
        int[] edgeNameArrayToTest = new int[10];
        while (traversal.hasNext()) {
            Array.set(edgeNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(edgeNameArray, edgeNameArrayToTest);
    }

    @Test
    public void testShort() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Vertex> vertexSet = new HashSet<>();
        Set<Short> vertexNameSet = new HashSet<>();
        for (Short i = 0; i < 10; i++) {
            vertexSet.add(this.sqlgGraph.addVertex(T.label, "A", "name", i));
            vertexNameSet.add(i);
        }
        this.sqlgGraph.tx().commit();
        testShort_assert(this.sqlgGraph, vertexSet, vertexNameSet);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testShort_assert(this.sqlgGraph1, vertexSet, vertexNameSet);
        }
    }

    private void testShort_assert(SqlgGraph sqlgGraph, Set<Vertex> vertexSet, Set<Short> vertexNameSet) {
        Set<Vertex> vertices = sqlgGraph.traversal().V().toSet();
        Set<Short> verticesName = sqlgGraph.traversal().V().<Short>values("name").toSet();
        assertEquals(10, vertices.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(vertexSet, vertices));
        assertTrue(CollectionUtils.isEqualCollection(vertexNameSet, verticesName));
    }

    @Test
    public void testShortEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Edge> edgeSet = new HashSet<>();
        Set<Short> edgeNameSet = new HashSet<>();
        for (Short i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            edgeSet.add(vertex1.addEdge("test", vertex2, "name", i));
            edgeNameSet.add(i);
        }
        this.sqlgGraph.tx().commit();
        testShortEdge_assert(this.sqlgGraph, edgeSet, edgeNameSet);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testShortEdge_assert(this.sqlgGraph1, edgeSet, edgeNameSet);
        }
    }

    private void testShortEdge_assert(SqlgGraph sqlgGraph, Set<Edge> edgeSet, Set<Short> edgeNameSet) {
        Set<Edge> edges = sqlgGraph.traversal().E().toSet();
        Set<Short> edgesName = sqlgGraph.traversal().E().<Short>values("name").toSet();
        assertEquals(10, edges.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(edgeSet, edges));
        assertTrue(CollectionUtils.isEqualCollection(edgeNameSet, edgesName));
    }

    @Test
    public void testShortPrimitive() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        short[] vertexNameArray = new short[10];
        for (short i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "name", i);
            Array.set(vertexNameArray, i, i);
        }
        this.sqlgGraph.tx().commit();
        testShortPrimitive_assert(this.sqlgGraph, vertexNameArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testShortPrimitive_assert(this.sqlgGraph1, vertexNameArray);
        }
    }

    private void testShortPrimitive_assert(SqlgGraph sqlgGraph, short[] vertexNameArray) {
        GraphTraversal<Vertex, Vertex> traversal = sqlgGraph.traversal().V();
        int count = 0;
        short[] vertexNameArrayToTest = new short[10];
        while (traversal.hasNext()) {
            Array.set(vertexNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(vertexNameArray, vertexNameArrayToTest);
    }

    @Test
    public void testShortPrimitiveEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        short[] edgeNameArray = new short[10];
        for (short i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            vertex1.addEdge("test", vertex2, "name", i);
            Array.set(edgeNameArray, i, i);
        }
        this.sqlgGraph.tx().commit();
        testShortPrimitiveEdge_assert(this.sqlgGraph, edgeNameArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testShortPrimitiveEdge_assert(this.sqlgGraph1, edgeNameArray);
        }
    }

    private void testShortPrimitiveEdge_assert(SqlgGraph sqlgGraph, short[] edgeNameArray) {
        GraphTraversal<Edge, Edge> traversal = sqlgGraph.traversal().E();
        int count = 0;
        short[] edgeNameArrayToTest = new short[10];
        while (traversal.hasNext()) {
            Array.set(edgeNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(edgeNameArray, edgeNameArrayToTest);
    }

    @Test
    public void testLong() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Vertex> vertexSet = new HashSet<>();
        Set<Long> vertexNameSet = new HashSet<>();
        for (long i = 0; i < 10; i++) {
            vertexSet.add(this.sqlgGraph.addVertex(T.label, "A", "name", Long.valueOf(i)));
            vertexNameSet.add(Long.valueOf(i));
        }
        this.sqlgGraph.tx().commit();
        testLong_assert(this.sqlgGraph, vertexSet, vertexNameSet);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testLong_assert(this.sqlgGraph1, vertexSet, vertexNameSet);
        }
    }

    private void testLong_assert(SqlgGraph sqlgGraph, Set<Vertex> vertexSet, Set<Long> vertexNameSet) {
        Set<Vertex> vertices = sqlgGraph.traversal().V().toSet();
        Set<Long> verticesName = sqlgGraph.traversal().V().<Long>values("name").toSet();
        assertEquals(10, vertices.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(vertexSet, vertices));
        assertTrue(CollectionUtils.isEqualCollection(vertexNameSet, verticesName));
    }

    @Test
    public void testLongEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Edge> edgeSet = new HashSet<>();
        Set<Long> edgeNameSet = new HashSet<>();
        for (long i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            edgeSet.add(vertex1.addEdge("test", vertex2, "name", Long.valueOf(i)));
            edgeNameSet.add(Long.valueOf(i));
        }
        this.sqlgGraph.tx().commit();
        testLongEdge_assert(this.sqlgGraph, edgeSet, edgeNameSet);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testLongEdge_assert(this.sqlgGraph1, edgeSet, edgeNameSet);
        }
    }

    private void testLongEdge_assert(SqlgGraph sqlgGraph, Set<Edge> edgeSet, Set<Long> edgeNameSet) {
        Set<Edge> edges = sqlgGraph.traversal().E().toSet();
        Set<Long> edgesName = sqlgGraph.traversal().E().<Long>values("name").toSet();
        assertEquals(10, edges.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(edgeSet, edges));
        assertTrue(CollectionUtils.isEqualCollection(edgeNameSet, edgesName));
    }

    @Test
    public void testLongPrimitive() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        long[] vertexNameArray = new long[10];
        for (long i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "name", i);
            Array.set(vertexNameArray, (int) i, i);
        }
        this.sqlgGraph.tx().commit();
        testLongPrimitive_assert(this.sqlgGraph, vertexNameArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testLongPrimitive_assert(this.sqlgGraph1, vertexNameArray);
        }
    }

    private void testLongPrimitive_assert(SqlgGraph sqlgGraph, long[] vertexNameArray) {
        GraphTraversal<Vertex, Vertex> traversal = sqlgGraph.traversal().V();
        int count = 0;
        long[] vertexNameArrayToTest = new long[10];
        while (traversal.hasNext()) {
            Array.set(vertexNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(vertexNameArray, vertexNameArrayToTest);
    }

    @Test
    public void testLongPrimitiveEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        long[] edgeNameArray = new long[10];
        for (long i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            vertex1.addEdge("test", vertex2, "name", i);
            Array.set(edgeNameArray, (int) i, i);
        }
        this.sqlgGraph.tx().commit();
        testLongPrimitiveEdge_assert(this.sqlgGraph, edgeNameArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testLongPrimitiveEdge_assert(this.sqlgGraph1, edgeNameArray);
        }
    }

    private void testLongPrimitiveEdge_assert(SqlgGraph sqlgGraph, long[] edgeNameArray) {
        GraphTraversal<Edge, Edge> traversal = sqlgGraph.traversal().E();
        int count = 0;
        long[] edgeNameArrayToTest = new long[10];
        while (traversal.hasNext()) {
            Array.set(edgeNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(edgeNameArray, edgeNameArrayToTest);
    }

    @Test
    public void testFloat() throws InterruptedException {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Vertex> vertexSet = new HashSet<>();
        Set<Float> vertexNameSet = new HashSet<>();
        for (float i = 0; i < 10; i++) {
            vertexSet.add(this.sqlgGraph.addVertex(T.label, "A", "name", Float.valueOf(i)));
            vertexNameSet.add(Float.valueOf(i));
        }
        this.sqlgGraph.tx().commit();
        testFloat_assert(this.sqlgGraph, vertexSet, vertexNameSet);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testFloat_assert(this.sqlgGraph1, vertexSet, vertexNameSet);
        }
    }

    private void testFloat_assert(SqlgGraph sqlgGraph, Set<Vertex> vertexSet, Set<Float> vertexNameSet) {
        Set<Vertex> vertices = sqlgGraph.traversal().V().toSet();
        Set<Float> verticesName = sqlgGraph.traversal().V().<Float>values("name").toSet();
        assertEquals(10, vertices.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(vertexSet, vertices));
        assertTrue(CollectionUtils.isEqualCollection(vertexNameSet, verticesName));
    }

    @Test
    public void testFloatEdge() throws InterruptedException {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Edge> edgeSet = new HashSet<>();
        Set<Float> edgeNameSet = new HashSet<>();
        for (float i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            edgeSet.add(vertex1.addEdge("test", vertex2, "name", Float.valueOf(i)));
            edgeNameSet.add(Float.valueOf(i));
        }
        this.sqlgGraph.tx().commit();
        testFloatEdge_assert(this.sqlgGraph, edgeSet, edgeNameSet);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testFloatEdge_assert(this.sqlgGraph1, edgeSet, edgeNameSet);
        }
    }

    private void testFloatEdge_assert(SqlgGraph sqlgGraph, Set<Edge> edgeSet, Set<Float> edgeNameSet) {
        Set<Edge> edges = sqlgGraph.traversal().E().toSet();
        Set<Float> edgesName = sqlgGraph.traversal().E().<Float>values("name").toSet();
        assertEquals(10, edges.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(edgeSet, edges));
        assertTrue(CollectionUtils.isEqualCollection(edgeNameSet, edgesName));
    }

    @Test
    public void testFloatPrimitive() throws InterruptedException {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        this.sqlgGraph.tx().normalBatchModeOn();
        float[] vertexNameArray = new float[10];
        for (float i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "name", i);
            Array.set(vertexNameArray, (int) i, i);
        }
        this.sqlgGraph.tx().commit();
        testFloatPrimitive_assert(this.sqlgGraph, vertexNameArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testFloatPrimitive_assert(this.sqlgGraph1, vertexNameArray);
        }
    }

    private void testFloatPrimitive_assert(SqlgGraph sqlgGraph, float[] vertexNameArray) {
        GraphTraversal<Vertex, Vertex> traversal = sqlgGraph.traversal().V();
        int count = 0;
        float[] vertexNameArrayToTest = new float[10];
        while (traversal.hasNext()) {
            Array.set(vertexNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(vertexNameArray, vertexNameArrayToTest, 0F);
    }

    @Test
    public void testFloatPrimitiveEdge() throws InterruptedException {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        this.sqlgGraph.tx().normalBatchModeOn();
        float[] edgeNameArray = new float[10];
        for (float i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            vertex1.addEdge("test", vertex2, "name", i);
            Array.set(edgeNameArray, (int) i, i);
        }
        this.sqlgGraph.tx().commit();
        testFloatPrimitiveEdge_assert(this.sqlgGraph, edgeNameArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testFloatPrimitiveEdge_assert(this.sqlgGraph1, edgeNameArray);
        }
    }

    private void testFloatPrimitiveEdge_assert(SqlgGraph sqlgGraph, float[] edgeNameArray) {
        GraphTraversal<Edge, Edge> traversal = sqlgGraph.traversal().E();
        int count = 0;
        float[] edgeNameArrayToTest = new float[10];
        while (traversal.hasNext()) {
            Array.set(edgeNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(edgeNameArray, edgeNameArrayToTest, 0F);
    }

    @Test
    public void testDouble() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Vertex> vertexSet = new HashSet<>();
        Set<Double> vertexNameSet = new HashSet<>();
        for (double i = 0; i < 10; i++) {
            vertexSet.add(this.sqlgGraph.addVertex(T.label, "A", "name", Double.valueOf(i)));
            vertexNameSet.add(Double.valueOf(i));
        }
        this.sqlgGraph.tx().commit();
        testDouble_assert(this.sqlgGraph, vertexSet, vertexNameSet);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testDouble_assert(this.sqlgGraph1, vertexSet, vertexNameSet);
        }
    }

    private void testDouble_assert(SqlgGraph sqlgGraph, Set<Vertex> vertexSet, Set<Double> vertexNameSet) {
        Set<Vertex> vertices = sqlgGraph.traversal().V().toSet();
        Set<Double> verticesName = sqlgGraph.traversal().V().<Double>values("name").toSet();
        assertEquals(10, vertices.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(vertexSet, vertices));
        assertTrue(CollectionUtils.isEqualCollection(vertexNameSet, verticesName));
    }

    @Test
    public void testDoubleEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Edge> edgeSet = new HashSet<>();
        Set<Double> edgeNameSet = new HashSet<>();
        for (double i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            edgeSet.add(vertex1.addEdge("test", vertex2, "name", Double.valueOf(i)));
            edgeNameSet.add(Double.valueOf(i));
        }
        this.sqlgGraph.tx().commit();
        testDoubleEdge_assert(this.sqlgGraph, edgeSet, edgeNameSet);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testDoubleEdge_assert(this.sqlgGraph1, edgeSet, edgeNameSet);
        }
    }

    private void testDoubleEdge_assert(SqlgGraph sqlgGraph, Set<Edge> edgeSet, Set<Double> edgeNameSet) {
        Set<Edge> edges = sqlgGraph.traversal().E().toSet();
        Set<Double> edgesName = sqlgGraph.traversal().E().<Double>values("name").toSet();
        assertEquals(10, edges.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(edgeSet, edges));
        assertTrue(CollectionUtils.isEqualCollection(edgeNameSet, edgesName));
    }

    @Test
    public void testDoublePrimitive() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        double[] edgeNameArray = new double[10];
        for (double i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A", "name", i);
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A", "name", i);
            vertex1.addEdge("test", vertex2, "name", i);
            Array.set(edgeNameArray, (int) i, i);
        }
        this.sqlgGraph.tx().commit();
        testDoublePrimitive_assert(this.sqlgGraph, edgeNameArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testDoublePrimitive_assert(this.sqlgGraph1, edgeNameArray);
        }
    }

    private void testDoublePrimitive_assert(SqlgGraph sqlgGraph, double[] edgeNameArray) {
        GraphTraversal<Edge, Edge> traversal = sqlgGraph.traversal().E();
        int count = 0;
        double[] edgeNameArrayToTest = new double[10];
        while (traversal.hasNext()) {
            Array.set(edgeNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(edgeNameArray, edgeNameArrayToTest, 0D);
    }

    @Test
    public void testDoublePrimitiveEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        double[] edgeNameArray = new double[10];
        for (double i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            vertex1.addEdge("test", vertex2, "name", i);
            Array.set(edgeNameArray, (int) i, i);
        }
        this.sqlgGraph.tx().commit();
        testDoublePrimitiveEdge_assert(this.sqlgGraph, edgeNameArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testDoublePrimitiveEdge_assert(this.sqlgGraph1, edgeNameArray);
        }
    }

    private void testDoublePrimitiveEdge_assert(SqlgGraph sqlgGraph, double[] edgeNameArray) {
        GraphTraversal<Edge, Edge> traversal = sqlgGraph.traversal().E();
        int count = 0;
        double[] edgeNameArrayToTest = new double[10];
        while (traversal.hasNext()) {
            Array.set(edgeNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(edgeNameArray, edgeNameArrayToTest, 0D);
    }


}
