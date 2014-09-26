package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlG;

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
                this.sqlG.getSqlDialect().supportsBooleanArrayValues() &&
                        this.sqlG.getSqlDialect().supportsFloatArrayValues() &&
                        this.sqlG.getSqlDialect().supportsLongArrayValues() &&
                        this.sqlG.getSqlDialect().supportsIntegerArrayValues() &&
                        this.sqlG.getSqlDialect().supportsDoubleArrayValues() &&
                        this.sqlG.getSqlDialect().supportsStringArrayValues()
        );
        this.sqlG.addVertex(T.label, "Person",
                "aBoolean", new boolean[]{true},
                "aShort", new short[]{(short) 1},
                "aInteger", new int[]{1},
                "aLong", new long[]{1L},
                "aFloat", new float[]{1F},
                "aDouble", new double[]{1D},
                "aString", new String[]{"aaaaaaaaaaaaa"});

        this.sqlG.tx().commit();
        this.sqlG.close();
        this.sqlG = SqlG.open(configuration);
        Iterator<Vertex> iter = this.sqlG.V().has(T.label, "Person");
        Assert.assertTrue(iter.hasNext());
        Vertex v = iter.next();
        Assert.assertTrue(Arrays.equals(new boolean[]{true}, (boolean[])v.property("aBoolean").value()));
        Assert.assertTrue(Arrays.equals(new short[]{(short) 1}, (short[])v.property("aShort").value()));
        Assert.assertTrue(Arrays.equals(new int[]{1}, (int[])v.property("aInteger").value()));
        Assert.assertTrue(Arrays.equals(new long[]{1l}, (long[])v.property("aLong").value()));
        Assert.assertTrue(Arrays.equals(new float[]{1f}, (float[])v.property("aFloat").value()));
        Assert.assertTrue(Arrays.equals(new double[]{1d}, (double[])v.property("aDouble").value()));
        Assert.assertTrue(Arrays.equals(new String[]{"aaaaaaaaaaaaa"}, (String[])v.property("aString").value()));

        this.sqlG.close();
        this.sqlG = SqlG.open(configuration);
        iter = this.sqlG.V().has(T.label, "Person");
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
                this.sqlG.getSqlDialect().supportsBooleanArrayValues() &&
                        this.sqlG.getSqlDialect().supportsLongArrayValues() &&
                        this.sqlG.getSqlDialect().supportsIntegerArrayValues() &&
                        this.sqlG.getSqlDialect().supportsDoubleArrayValues() &&
                        this.sqlG.getSqlDialect().supportsStringArrayValues()
        );
        this.sqlG.addVertex(T.label, "Person",
                "aBoolean", new boolean[]{true},
                "aShort", new short[]{(short) 1},
                "aInteger", new int[]{1},
                "aLong", new long[]{1L},
                "aDouble", new double[]{1D},
                "aString", new String[]{"aaaaaaaaaaaaa"});

        this.sqlG.tx().commit();
        this.sqlG.close();
        this.sqlG = SqlG.open(configuration);
        Iterator<Vertex> iter = this.sqlG.V().has(T.label, "Person");
        Assert.assertTrue(iter.hasNext());
        Vertex v = iter.next();
        Assert.assertTrue(Arrays.equals(new boolean[]{true}, (boolean[])v.property("aBoolean").value()));
        Assert.assertTrue(Arrays.equals(new short[]{(short) 1}, (short[])v.property("aShort").value()));
        Assert.assertTrue(Arrays.equals(new int[]{1}, (int[])v.property("aInteger").value()));
        Assert.assertTrue(Arrays.equals(new long[]{1l}, (long[])v.property("aLong").value()));
        Assert.assertTrue(Arrays.equals(new double[]{1d}, (double[])v.property("aDouble").value()));
        Assert.assertTrue(Arrays.equals(new String[]{"aaaaaaaaaaaaa"}, (String[])v.property("aString").value()));

        this.sqlG.close();
        this.sqlG = SqlG.open(configuration);
        iter = this.sqlG.V().has(T.label, "Person");
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
