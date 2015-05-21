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
                "age4", 1l,
                "age5", 1f,
                "age6", 1d
        );
        this.sqlgGraph.tx().commit();
        Assert.assertEquals((short)1, v.property("age2").value());
        Assert.assertEquals(1, v.property("age3").value());
        Assert.assertEquals(1l, v.property("age4").value());
        Assert.assertEquals(1f, v.property("age5").value());
        Assert.assertEquals(1d, v.property("age6").value());
    }

    @Test
    public void testPrimitivePropertiesNoFloat() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person",
                "age2", (short)1,
                "age3", 1,
                "age4", 1l,
                "age6", 1d
        );
        this.sqlgGraph.tx().commit();
        Assert.assertEquals((short)1, v.property("age2").value());
        Assert.assertEquals(1, v.property("age3").value());
        Assert.assertEquals(1l, v.property("age4").value());
        Assert.assertEquals(1d, v.property("age6").value());
    }

    @Test
    public void testObjectProperties() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsFloatValues());
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person",
                "age2", new Short((short)1),
                "age3", new Integer(1),
                "age4", new Long(1l),
                "age5", new Float(1f),
                "age6", new Double(1d)
        );
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(new Short((short)1), v.property("age2").value());
        Assert.assertEquals(new Integer(1), v.property("age3").value());
        Assert.assertEquals(new Long(1l), v.property("age4").value());
        Assert.assertEquals(new Float(1f), v.property("age5").value());
        Assert.assertEquals(new Double(1d), v.property("age6").value());
    }

    @Test
    public void testObjectPropertiesNoFloat() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person",
                "age2", new Short((short)1),
                "age3", new Integer(1),
                "age4", new Long(1l),
                "age6", new Double(1d)
        );
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(new Short((short)1), v.property("age2").value());
        Assert.assertEquals(new Integer(1), v.property("age3").value());
        Assert.assertEquals(new Long(1l), v.property("age4").value());
        Assert.assertEquals(new Double(1d), v.property("age6").value());
    }

}
