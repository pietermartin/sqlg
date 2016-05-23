package org.umlg.sqlg.test.batch;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new String[]{"name1", "name2"});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Iterator<Object> array = god.values("array");
        assertTrue(array.hasNext());
        Object o = array.next();
        assertTrue(o instanceof String[]);
        List<String> list = Arrays.asList((String[]) o);
        assertEquals(2, list.size());
        assertTrue(list.contains("name1"));
        assertTrue(list.contains("name2"));
    }

    @Test
    public void testBatchArrayInteger() {
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new int[]{1, 2});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Iterator<Object> array = god.values("array");
        assertTrue(array.hasNext());
        Object o = array.next();
        assertTrue(o instanceof int[]);
        List<Integer> list = Arrays.asList(ArrayUtils.toObject((int[]) o));
        assertEquals(2, list.size());
        assertTrue(list.contains(1));
        assertTrue(list.contains(2));
    }

    @Test
    public void testBatchArrayDouble() {
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new double[]{1, 2});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Iterator<Object> array = god.values("array");
        assertTrue(array.hasNext());
        Object o = array.next();
        assertTrue(o instanceof double[]);
        List<Double> list = Arrays.asList(ArrayUtils.toObject((double[]) o));
        assertEquals(2, list.size());
        assertTrue(list.contains(1d));
        assertTrue(list.contains(2d));
    }





}
