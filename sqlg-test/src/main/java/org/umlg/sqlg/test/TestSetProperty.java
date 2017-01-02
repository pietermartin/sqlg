package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 7:48 PM
 */
public class TestSetProperty extends BaseTest {

    @Test
    public void testSetByteProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsByteValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("byte", new Byte((byte)1));
        this.sqlgGraph.tx().commit();
        Assert.assertEquals((byte)1, marko.property("byte").value());
    }
    
    @Test
    public void testSetBooleanArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBooleanArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("bools", new Boolean[]{true,false});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new Boolean[]{true,false}, (Boolean[]) marko.property("bools").value());
    }
    
    
    @Test
    public void testSetByteArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsByteArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("bytes", new Byte[]{(byte)1,(byte)2});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new Byte[]{(byte)1,(byte)2}, (Byte[]) marko.property("bytes").value());
    }
    
    @Test
    public void testSetDoubleArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDoubleArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("doubles", new Double[]{1.0,2.2});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new Double[]{1.0,2.2}, (Double[]) marko.property("doubles").value());
    }
    
    @Test
    public void testSetFloatArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("floats", new Float[]{1.0f,2.2f});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new Float[]{1.0f,2.2f}, (Float[]) marko.property("floats").value());
    }
    
    @Test
    public void testSetIntegerArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("integers", new Integer[]{1,2});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new Integer[]{1,2}, (Integer[]) marko.property("integers").value());
    }
    
    @Test
    public void testSetLongArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLongArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("longs", new Long[]{1L,2L});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new Long[]{1L,2L}, (Long[]) marko.property("longs").value());
    }
    
    @Test
    public void testSetShortArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsShortArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("shorts", new Short[]{1,2});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new Short[]{1,2}, (Short[]) marko.property("shorts").value());
    }
    
    @Test
    public void testSetBooleanPrimitiveArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBooleanArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("bools", new boolean[]{true,false});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new boolean[]{true,false}, (boolean[]) marko.property("bools").value());
    }
    
    @Test
    public void testSetBytePrimitiveArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsByteArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("bytes", new byte[]{(byte)1,(byte)2});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new byte[]{(byte)1,(byte)2}, (byte[]) marko.property("bytes").value());
    }
    
    @Test
    public void testSetDoublePrimitiveArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDoubleArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("doubles", new double[]{1.0,2.2});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new double[]{1.0,2.2}, (double[]) marko.property("doubles").value(),0.00001);
    }
    
    @Test
    public void testSetFloatPrimitiveArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("floats", new float[]{1.0f,2.2f});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new float[]{1.0f,2.2f}, (float[]) marko.property("floats").value(),0.00001f);
    }
    
    @Test
    public void testSetIntegerPrimitiveArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("integers", new int[]{1,2});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new int[]{1,2}, (int[]) marko.property("integers").value());
    }
    
    @Test
    public void testSetLongPrimitiveArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLongArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("longs", new long[]{1L,2L});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new long[]{1L,2L}, (long[]) marko.property("longs").value());
    }
    
    @Test
    public void testSetShortPrimitiveArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsShortArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("shorts", new short[]{1,2});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new short[]{1,2}, (short[]) marko.property("shorts").value());
    }
    
    
    @Test
    public void testSetStringArrayProperty() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStringArrayValues());
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("strings", new String[]{"a","b"});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new String[]{"a","b"}, (String[]) marko.property("strings").value());
    }
    
    
    @Test
    public void testSetPrimitiveProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        v.property( "age2", (short)1);
        v.property( "age3", 1);
        v.property( "age4", 1L);
        v.property( "age5", 1f);
        v.property( "age6", 1d);
        v.property( "ok", true);
        
        this.sqlgGraph.tx().commit();
        Assert.assertEquals((short)1, v.property("age2").value());
        Assert.assertEquals(1, v.property("age3").value());
        Assert.assertEquals(1L, v.property("age4").value());
        Assert.assertEquals(1f, v.property("age5").value());
        Assert.assertEquals(1d, v.property("age6").value());
        Assert.assertEquals(true, v.property("ok").value());
        
    }
    
    @Test
    public void testSetObjectProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        v.property( "age2", new Short((short)1));
        v.property( "age3", new Integer(1));
        v.property( "age4", new Long(1L));
        v.property( "age5", new Float(1f));
        v.property( "age6", new Double(1d));
        v.property( "ok", Boolean.TRUE);
        
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(new Short((short)1), v.property("age2").value());
        Assert.assertEquals(new Integer(1), v.property("age3").value());
        Assert.assertEquals(new Long(1L), v.property("age4").value());
        Assert.assertEquals(new Float(1f), v.property("age5").value());
        Assert.assertEquals(new Double(1d), v.property("age6").value());
        Assert.assertEquals(Boolean.TRUE, v.property("ok").value());
       
    }

    @Test
    public void testSetProperty() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        marko.property("surname", "xxxx");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("xxxx", marko.property("surname").value());
    }

    @Test
    public void testPropertyManyTimes() {
    	Vertex v = this.sqlgGraph.addVertex("age", 1, "name", "marko", "name", "john");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("john", v.property("name").value());
    }
    
    @Test
    public void testSetPropertyManyTimes() {
    	Vertex v = this.sqlgGraph.addVertex("age", 1, "name", "marko");
    	v.property("name","tony");
    	v.property("name","john");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("john", v.property("name").value());
    }

    @Test
    public void testFloat() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "age", 1f);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1f, v.property("age").value());
    }

    @Test
    public void testPrimitiveProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person",
                "age2", (short)1,
                "age3", 1,
                "age4", 1L,
                "age5", 1f,
                "age6", 1d,
                "ok", true
        );
        this.sqlgGraph.tx().commit();
        Assert.assertEquals((short)1, v.property("age2").value());
        Assert.assertEquals(1, v.property("age3").value());
        Assert.assertEquals(1L, v.property("age4").value());
        Assert.assertEquals(1f, v.property("age5").value());
        Assert.assertEquals(1d, v.property("age6").value());
        Assert.assertEquals(true, v.property("ok").value());
     }

    @Test
    public void testPrimitivePropertiesNoFloat() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person",
                "age2", (short)1,
                "age3", 1,
                "age4", 1L,
                "age6", 1d,
                "ok", true
        );
        this.sqlgGraph.tx().commit();
        Assert.assertEquals((short)1, v.property("age2").value());
        Assert.assertEquals(1, v.property("age3").value());
        Assert.assertEquals(1L, v.property("age4").value());
        Assert.assertEquals(1d, v.property("age6").value());
        Assert.assertEquals(true, v.property("ok").value());
    }

    @Test
    public void testObjectProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person",
                "age2", new Short((short)1),
                "age3", new Integer(1),
                "age4", new Long(1L),
                "age5", new Float(1f),
                "age6", new Double(1d),
                "ok", Boolean.TRUE
        );
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(new Short((short)1), v.property("age2").value());
        Assert.assertEquals(new Integer(1), v.property("age3").value());
        Assert.assertEquals(new Long(1L), v.property("age4").value());
        Assert.assertEquals(new Float(1f), v.property("age5").value());
        Assert.assertEquals(new Double(1d), v.property("age6").value());
        Assert.assertEquals(Boolean.TRUE, v.property("ok").value());
    }

    @Test
    public void testObjectPropertiesNoFloat() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person",
                "age2", new Short((short)1),
                "age3", new Integer(1),
                "age4", new Long(1L),
                "age6", new Double(1d),
                "ok", Boolean.TRUE
        );
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(new Short((short)1), v.property("age2").value());
        Assert.assertEquals(new Integer(1), v.property("age3").value());
        Assert.assertEquals(new Long(1L), v.property("age4").value());
        Assert.assertEquals(new Double(1d), v.property("age6").value());
        Assert.assertEquals(Boolean.TRUE, v.property("ok").value());
    }

}
