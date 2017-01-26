package org.umlg.sqlg.test.batch;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
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

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testString() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Vertex> vertexSet = new HashSet<>();
        Set<String> vertexNameSet = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String s = "name" + i;
            vertexSet.add(this.sqlgGraph.addVertex(T.label, "A", "name", s));
            vertexNameSet.add(s);
        }
        this.sqlgGraph.tx().commit();
        Set<Vertex> vertices = this.sqlgGraph.traversal().V().toSet();
        Set<String> verticesName = this.sqlgGraph.traversal().V().<String>values("name").toSet();
        assertEquals(10, vertices.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(vertexSet, vertices));
        assertTrue(CollectionUtils.isEqualCollection(vertexNameSet, verticesName));
    }

    @Test
    public void testStringEdge() {
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
        Set<Edge> edges = this.sqlgGraph.traversal().E().toSet();
        Set<String> edgesName = this.sqlgGraph.traversal().E().<String>values("name").toSet();
        assertEquals(10, edges.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(edgeSet, edges));
        assertTrue(CollectionUtils.isEqualCollection(edgeNameSet, edgesName));
    }

    @Test
    public void testBoolean() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Vertex> vertexSet = new HashSet<>();
        Set<Boolean> vertexNameSet = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            Boolean b = i % 2 == 0;
            ;
            vertexSet.add(this.sqlgGraph.addVertex(T.label, "A", "name", new Boolean(b)));
            vertexNameSet.add(new Boolean(b));
        }
        this.sqlgGraph.tx().commit();
        Set<Vertex> vertices = this.sqlgGraph.traversal().V().toSet();
        Set<Boolean> verticesName = this.sqlgGraph.traversal().V().<Boolean>values("name").toSet();
        assertEquals(10, vertices.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(vertexSet, vertices));
        assertTrue(CollectionUtils.isEqualCollection(vertexNameSet, verticesName));
    }

    @Test
    public void testBooleanEdge() {
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
        Set<Edge> edges = this.sqlgGraph.traversal().E().toSet();
        Set<Boolean> edgesName = this.sqlgGraph.traversal().E().<Boolean>values("name").toSet();
        assertEquals(10, edges.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(edgeSet, edges));
        assertTrue(CollectionUtils.isEqualCollection(edgeNameSet, edgesName));
    }

    @Test
    public void testBooleanPrimitive() {
        this.sqlgGraph.tx().normalBatchModeOn();
        boolean[] vertexNameArray = new boolean[10];
        for (int i = 0; i < 10; i++) {
            boolean b = i % 2 == 0;
            this.sqlgGraph.addVertex(T.label, "A", "name", b);
            Array.set(vertexNameArray, i, b);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V();
        int count = 0;
        boolean[] vertexNameArrayToTest = new boolean[10];
        while (traversal.hasNext()) {
            Array.set(vertexNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(vertexNameArray, vertexNameArrayToTest);
    }

    @Test
    public void testBooleanPrimitiveEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        boolean[] edgeNameArray = new boolean[10];
        for (int i = 0; i < 10; i++) {
            boolean b = i % 2 == 0;
            ;
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            vertex1.addEdge("test", vertex2, "name", b);
            Array.set(edgeNameArray, i, b);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Edge, Edge> traversal = this.sqlgGraph.traversal().E();
        int count = 0;
        boolean[] edgeNameArrayToTest = new boolean[10];
        while (traversal.hasNext()) {
            Array.set(edgeNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(edgeNameArray, edgeNameArrayToTest);
    }

    @Test
    public void testInteger() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Vertex> vertexSet = new HashSet<>();
        Set<Integer> vertexNameSet = new HashSet<>();
        for (Integer i = 0; i < 10; i++) {
            vertexSet.add(this.sqlgGraph.addVertex(T.label, "A", "name", i));
            vertexNameSet.add(i);
        }
        this.sqlgGraph.tx().commit();
        Set<Vertex> vertices = this.sqlgGraph.traversal().V().toSet();
        Set<Integer> verticesName = this.sqlgGraph.traversal().V().<Integer>values("name").toSet();
        assertEquals(10, vertices.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(vertexSet, vertices));
        assertTrue(CollectionUtils.isEqualCollection(vertexNameSet, verticesName));
    }

    @Test
    public void testIntegerEdge() {
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
        Set<Edge> edges = this.sqlgGraph.traversal().E().toSet();
        Set<Integer> edgesName = this.sqlgGraph.traversal().E().<Integer>values("name").toSet();
        assertEquals(10, edges.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(edgeSet, edges));
        assertTrue(CollectionUtils.isEqualCollection(edgeNameSet, edgesName));
    }

    @Test
    public void testIntegerPrimitive() {
        this.sqlgGraph.tx().normalBatchModeOn();
        int[] vertexNameArray = new int[10];
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "name", i);
            Array.set(vertexNameArray, i, i);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V();
        int count = 0;
        int[] vertexNameArrayToTest = new int[10];
        while (traversal.hasNext()) {
            Array.set(vertexNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(vertexNameArray, vertexNameArrayToTest);
    }

    @Test
    public void testIntegerPrimitiveEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        int[] edgeNameArray = new int[10];
        for (int i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            vertex1.addEdge("test", vertex2, "name", i);
            Array.set(edgeNameArray, i, i);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Edge, Edge> traversal = this.sqlgGraph.traversal().E();
        int count = 0;
        int[] edgeNameArrayToTest = new int[10];
        while (traversal.hasNext()) {
            Array.set(edgeNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(edgeNameArray, edgeNameArrayToTest);
    }

    @Test
    public void testShort() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Vertex> vertexSet = new HashSet<>();
        Set<Short> vertexNameSet = new HashSet<>();
        for (Short i = 0; i < 10; i++) {
            vertexSet.add(this.sqlgGraph.addVertex(T.label, "A", "name", i));
            vertexNameSet.add(i);
        }
        this.sqlgGraph.tx().commit();
        Set<Vertex> vertices = this.sqlgGraph.traversal().V().toSet();
        Set<Short> verticesName = this.sqlgGraph.traversal().V().<Short>values("name").toSet();
        assertEquals(10, vertices.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(vertexSet, vertices));
        assertTrue(CollectionUtils.isEqualCollection(vertexNameSet, verticesName));
    }

    @Test
    public void testShortEdge() {
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
        Set<Edge> edges = this.sqlgGraph.traversal().E().toSet();
        Set<Short> edgesName = this.sqlgGraph.traversal().E().<Short>values("name").toSet();
        assertEquals(10, edges.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(edgeSet, edges));
        assertTrue(CollectionUtils.isEqualCollection(edgeNameSet, edgesName));
    }

    @Test
    public void testShortPrimitive() {
        this.sqlgGraph.tx().normalBatchModeOn();
        short[] vertexNameArray = new short[10];
        for (short i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "name", i);
            Array.set(vertexNameArray, i, i);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V();
        int count = 0;
        short[] vertexNameArrayToTest = new short[10];
        while (traversal.hasNext()) {
            Array.set(vertexNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(vertexNameArray, vertexNameArrayToTest);
    }

    @Test
    public void testShortPrimitiveEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        short[] edgeNameArray = new short[10];
        for (short i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            vertex1.addEdge("test", vertex2, "name", i);
            Array.set(edgeNameArray, i, i);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Edge, Edge> traversal = this.sqlgGraph.traversal().E();
        int count = 0;
        short[] edgeNameArrayToTest = new short[10];
        while (traversal.hasNext()) {
            Array.set(edgeNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(edgeNameArray, edgeNameArrayToTest);
    }

    @Test
    public void testLong() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Vertex> vertexSet = new HashSet<>();
        Set<Long> vertexNameSet = new HashSet<>();
        for (long i = 0; i < 10; i++) {
            vertexSet.add(this.sqlgGraph.addVertex(T.label, "A", "name", Long.valueOf(i)));
            vertexNameSet.add(Long.valueOf(i));
        }
        this.sqlgGraph.tx().commit();
        Set<Vertex> vertices = this.sqlgGraph.traversal().V().toSet();
        Set<Long> verticesName = this.sqlgGraph.traversal().V().<Long>values("name").toSet();
        assertEquals(10, vertices.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(vertexSet, vertices));
        assertTrue(CollectionUtils.isEqualCollection(vertexNameSet, verticesName));
    }

    @Test
    public void testLongEdge() {
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
        Set<Edge> edges = this.sqlgGraph.traversal().E().toSet();
        Set<Long> edgesName = this.sqlgGraph.traversal().E().<Long>values("name").toSet();
        assertEquals(10, edges.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(edgeSet, edges));
        assertTrue(CollectionUtils.isEqualCollection(edgeNameSet, edgesName));
    }

    @Test
    public void testLongPrimitive() {
        this.sqlgGraph.tx().normalBatchModeOn();
        long[] vertexNameArray = new long[10];
        for (long i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "name", i);
            Array.set(vertexNameArray, (int) i, i);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V();
        int count = 0;
        long[] vertexNameArrayToTest = new long[10];
        while (traversal.hasNext()) {
            Array.set(vertexNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(vertexNameArray, vertexNameArrayToTest);
    }

    @Test
    public void testLongPrimitiveEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        long[] edgeNameArray = new long[10];
        for (long i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            vertex1.addEdge("test", vertex2, "name", i);
            Array.set(edgeNameArray, (int) i, i);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Edge, Edge> traversal = this.sqlgGraph.traversal().E();
        int count = 0;
        long[] edgeNameArrayToTest = new long[10];
        while (traversal.hasNext()) {
            Array.set(edgeNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(edgeNameArray, edgeNameArrayToTest);
    }

    @Test
    public void testFloat() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Vertex> vertexSet = new HashSet<>();
        Set<Float> vertexNameSet = new HashSet<>();
        for (float i = 0; i < 10; i++) {
            vertexSet.add(this.sqlgGraph.addVertex(T.label, "A", "name", Float.valueOf(i)));
            vertexNameSet.add(Float.valueOf(i));
        }
        this.sqlgGraph.tx().commit();
        Set<Vertex> vertices = this.sqlgGraph.traversal().V().toSet();
        Set<Float> verticesName = this.sqlgGraph.traversal().V().<Float>values("name").toSet();
        assertEquals(10, vertices.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(vertexSet, vertices));
        assertTrue(CollectionUtils.isEqualCollection(vertexNameSet, verticesName));
    }

    @Test
    public void testFloatEdge() {
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
        Set<Edge> edges = this.sqlgGraph.traversal().E().toSet();
        Set<Float> edgesName = this.sqlgGraph.traversal().E().<Float>values("name").toSet();
        assertEquals(10, edges.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(edgeSet, edges));
        assertTrue(CollectionUtils.isEqualCollection(edgeNameSet, edgesName));
    }

    @Test
    public void testFloatPrimitive() {
        this.sqlgGraph.tx().normalBatchModeOn();
        float[] vertexNameArray = new float[10];
        for (float i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "name", i);
            Array.set(vertexNameArray, (int) i, i);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V();
        int count = 0;
        float[] vertexNameArrayToTest = new float[10];
        while (traversal.hasNext()) {
            Array.set(vertexNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(vertexNameArray, vertexNameArrayToTest, 0F);
    }

    @Test
    public void testFloatPrimitiveEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        float[] edgeNameArray = new float[10];
        for (float i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            vertex1.addEdge("test", vertex2, "name", i);
            Array.set(edgeNameArray, (int) i, i);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Edge, Edge> traversal = this.sqlgGraph.traversal().E();
        int count = 0;
        float[] edgeNameArrayToTest = new float[10];
        while (traversal.hasNext()) {
            Array.set(edgeNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(edgeNameArray, edgeNameArrayToTest, 0F);
    }

    @Test
    public void testDouble() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Vertex> vertexSet = new HashSet<>();
        Set<Double> vertexNameSet = new HashSet<>();
        for (double i = 0; i < 10; i++) {
            vertexSet.add(this.sqlgGraph.addVertex(T.label, "A", "name", Double.valueOf(i)));
            vertexNameSet.add(Double.valueOf(i));
        }
        this.sqlgGraph.tx().commit();
        Set<Vertex> vertices = this.sqlgGraph.traversal().V().toSet();
        Set<Double> verticesName = this.sqlgGraph.traversal().V().<Double>values("name").toSet();
        assertEquals(10, vertices.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(vertexSet, vertices));
        assertTrue(CollectionUtils.isEqualCollection(vertexNameSet, verticesName));
    }

    @Test
    public void testDoubleEdge() {
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
        Set<Edge> edges = this.sqlgGraph.traversal().E().toSet();
        Set<Double> edgesName = this.sqlgGraph.traversal().E().<Double>values("name").toSet();
        assertEquals(10, edges.size(), 0);
        assertTrue(CollectionUtils.isEqualCollection(edgeSet, edges));
        assertTrue(CollectionUtils.isEqualCollection(edgeNameSet, edgesName));
    }

    @Test
    public void testDoublePrimitive() {
        this.sqlgGraph.tx().normalBatchModeOn();
        double[] edgeNameArray = new double[10];
        for (double i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A", "name", i);
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A", "name", i);
            vertex1.addEdge("test", vertex2, "name", i);
            Array.set(edgeNameArray, (int) i, i);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Edge, Edge> traversal = this.sqlgGraph.traversal().E();
        int count = 0;
        double[] edgeNameArrayToTest = new double[10];
        while (traversal.hasNext()) {
            Array.set(edgeNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(edgeNameArray, edgeNameArrayToTest, 0D);
    }

    @Test
    public void testDoublePrimitiveEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        double[] edgeNameArray = new double[10];
        for (double i = 0; i < 10; i++) {
            Vertex vertex1 = this.sqlgGraph.addVertex(T.label, "A");
            Vertex vertex2 = this.sqlgGraph.addVertex(T.label, "A");
            vertex1.addEdge("test", vertex2, "name", i);
            Array.set(edgeNameArray, (int) i, i);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Edge, Edge> traversal = this.sqlgGraph.traversal().E();
        int count = 0;
        double[] edgeNameArrayToTest = new double[10];
        while (traversal.hasNext()) {
            Array.set(edgeNameArrayToTest, count++, traversal.next().value("name"));
        }
        assertArrayEquals(edgeNameArray, edgeNameArrayToTest, 0D);
    }


}
