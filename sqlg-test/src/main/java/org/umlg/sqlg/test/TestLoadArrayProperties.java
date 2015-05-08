package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Date: 2014/07/22
 * Time: 3:40 PM
 */
public class TestLoadArrayProperties extends BaseTest {

    @Test
    public void testLoadSchemaWithArrays() throws Exception {
        Assume.assumeTrue(
                this.sqlgGraph.getSqlDialect().supportsBooleanArrayValues() &&
                        this.sqlgGraph.getSqlDialect().supportsFloatArrayValues() &&
                        this.sqlgGraph.getSqlDialect().supportsLongArrayValues() &&
                        this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues() &&
                        this.sqlgGraph.getSqlDialect().supportsDoubleArrayValues() &&
                        this.sqlgGraph.getSqlDialect().supportsStringArrayValues()
        );
        this.sqlgGraph.addVertex(T.label, "Person",
                "aBoolean", new boolean[]{true},
                "aShort", new short[]{(short) 1},
                "aInteger", new int[]{1},
                "aLong", new long[]{1L},
                "aFloat", new float[]{1F},
                "aDouble", new double[]{1D},
                "aString", new String[]{"aaaaaaaaaaaaa"});

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        Iterator<Vertex> iter = this.sqlgGraph.traversal().V().has(T.label, "Person");
        Assert.assertTrue(iter.hasNext());
        Vertex v = iter.next();
        Assert.assertTrue(Arrays.equals(new boolean[]{true}, (boolean[])v.property("aBoolean").value()));
        Assert.assertTrue(Arrays.equals(new short[]{(short) 1}, (short[])v.property("aShort").value()));
        Assert.assertTrue(Arrays.equals(new int[]{1}, (int[])v.property("aInteger").value()));
        Assert.assertTrue(Arrays.equals(new long[]{1l}, (long[])v.property("aLong").value()));
        Assert.assertTrue(Arrays.equals(new float[]{1f}, (float[])v.property("aFloat").value()));
        Assert.assertTrue(Arrays.equals(new double[]{1d}, (double[])v.property("aDouble").value()));
        Assert.assertTrue(Arrays.equals(new String[]{"aaaaaaaaaaaaa"}, (String[])v.property("aString").value()));

        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        iter = this.sqlgGraph.traversal().V().has(T.label, "Person");
        Assert.assertTrue(iter.hasNext());
        v = iter.next();
        Assert.assertTrue(Arrays.equals(new boolean[]{true}, (boolean[])v.property("aBoolean").value()));
        Assert.assertTrue(Arrays.equals(new short[]{(short) 1}, (short[])v.property("aShort").value()));
        Assert.assertTrue(Arrays.equals(new int[]{1}, (int[])v.property("aInteger").value()));
        Assert.assertTrue(Arrays.equals(new long[]{1l}, (long[])v.property("aLong").value()));
        Assert.assertTrue(Arrays.equals(new float[]{1f}, (float[])v.property("aFloat").value()));
        Assert.assertTrue(Arrays.equals(new double[]{1d}, (double[])v.property("aDouble").value()));
        Assert.assertTrue(Arrays.equals(new String[]{"aaaaaaaaaaaaa"}, (String[])v.property("aString").value()));
    }

    @Test
    public void testLoadSchemaWithArraysWithoutFloat() throws Exception {
        Assume.assumeTrue(
                this.sqlgGraph.getSqlDialect().supportsBooleanArrayValues() &&
                        this.sqlgGraph.getSqlDialect().supportsLongArrayValues() &&
                        this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues() &&
                        this.sqlgGraph.getSqlDialect().supportsDoubleArrayValues() &&
                        this.sqlgGraph.getSqlDialect().supportsStringArrayValues()
        );
        this.sqlgGraph.addVertex(T.label, "Person",
                "aBoolean", new boolean[]{true},
                "aShort", new short[]{(short) 1},
                "aInteger", new int[]{1},
                "aLong", new long[]{1L},
                "aDouble", new double[]{1D},
                "aString", new String[]{"aaaaaaaaaaaaa"});

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        Iterator<Vertex> iter = this.sqlgGraph.traversal().V().has(T.label, "Person");
        Assert.assertTrue(iter.hasNext());
        Vertex v = iter.next();
        Assert.assertTrue(Arrays.equals(new boolean[]{true}, (boolean[])v.property("aBoolean").value()));
        Assert.assertTrue(Arrays.equals(new short[]{(short) 1}, (short[])v.property("aShort").value()));
        Assert.assertTrue(Arrays.equals(new int[]{1}, (int[])v.property("aInteger").value()));
        Assert.assertTrue(Arrays.equals(new long[]{1l}, (long[])v.property("aLong").value()));
        Assert.assertTrue(Arrays.equals(new double[]{1d}, (double[])v.property("aDouble").value()));
        Assert.assertTrue(Arrays.equals(new String[]{"aaaaaaaaaaaaa"}, (String[])v.property("aString").value()));

        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        iter = this.sqlgGraph.traversal().V().has(T.label, "Person");
        Assert.assertTrue(iter.hasNext());
        v = iter.next();
        Assert.assertTrue(Arrays.equals(new boolean[]{true}, (boolean[])v.property("aBoolean").value()));
        Assert.assertTrue(Arrays.equals(new short[]{(short) 1}, (short[])v.property("aShort").value()));
        Assert.assertTrue(Arrays.equals(new int[]{1}, (int[])v.property("aInteger").value()));
        Assert.assertTrue(Arrays.equals(new long[]{1l}, (long[])v.property("aLong").value()));
        Assert.assertTrue(Arrays.equals(new double[]{1d}, (double[])v.property("aDouble").value()));
        Assert.assertTrue(Arrays.equals(new String[]{"aaaaaaaaaaaaa"}, (String[])v.property("aString").value()));
    }

}
