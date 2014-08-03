package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 7:48 PM
 */
public class TestSetProperty extends BaseTest {

    @Test
    public void testSetProperty() {
        Vertex marko = this.sqlG.addVertex(Element.LABEL, "Person", "name", "marko");
        marko.property("surname", "xxxx");
        this.sqlG.tx().commit();
        Assert.assertEquals("xxxx", marko.property("surname").value());
    }

    @Test
    public void testPropertyManyTimes() {
        Vertex v = this.sqlG.addVertex("age", 1, "name", "marko", "name", "john");
        this.sqlG.tx().commit();
    }

    @Test
    public void testFloat() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsFloatValues());
        Vertex v = this.sqlG.addVertex(Element.LABEL, "Person", "age", 1f);
        this.sqlG.tx().commit();
        Assert.assertEquals(1f, v.property("age").value());
    }

    @Test
    public void testPrimitiveProperties() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsFloatValues());
        Vertex v = this.sqlG.addVertex(Element.LABEL, "Person",
                "age2", (short)1,
                "age3", 1,
                "age4", 1l,
                "age5", 1f,
                "age6", 1d
        );
        this.sqlG.tx().commit();
        Assert.assertEquals((short)1, v.property("age2").value());
        Assert.assertEquals(1, v.property("age3").value());
        Assert.assertEquals(1l, v.property("age4").value());
        Assert.assertEquals(1f, v.property("age5").value());
        Assert.assertEquals(1d, v.property("age6").value());
    }

    @Test
    public void testPrimitivePropertiesNoFloat() {
        Vertex v = this.sqlG.addVertex(Element.LABEL, "Person",
                "age2", (short)1,
                "age3", 1,
                "age4", 1l,
                "age6", 1d
        );
        this.sqlG.tx().commit();
        Assert.assertEquals((short)1, v.property("age2").value());
        Assert.assertEquals(1, v.property("age3").value());
        Assert.assertEquals(1l, v.property("age4").value());
        Assert.assertEquals(1d, v.property("age6").value());
    }

    @Test
    public void testObjectProperties() {
        Assume.assumeTrue(this.sqlG.getSqlDialect().supportsFloatValues());
        Vertex v = this.sqlG.addVertex(Element.LABEL, "Person",
                "age2", new Short((short)1),
                "age3", new Integer(1),
                "age4", new Long(1l),
                "age5", new Float(1f),
                "age6", new Double(1d)
        );
        this.sqlG.tx().commit();
        Assert.assertEquals(new Short((short)1), v.property("age2").value());
        Assert.assertEquals(new Integer(1), v.property("age3").value());
        Assert.assertEquals(new Long(1l), v.property("age4").value());
        Assert.assertEquals(new Float(1f), v.property("age5").value());
        Assert.assertEquals(new Double(1d), v.property("age6").value());
    }

    @Test
    public void testObjectPropertiesNoFloat() {
        Vertex v = this.sqlG.addVertex(Element.LABEL, "Person",
                "age2", new Short((short)1),
                "age3", new Integer(1),
                "age4", new Long(1l),
                "age6", new Double(1d)
        );
        this.sqlG.tx().commit();
        Assert.assertEquals(new Short((short)1), v.property("age2").value());
        Assert.assertEquals(new Integer(1), v.property("age3").value());
        Assert.assertEquals(new Long(1l), v.property("age4").value());
        Assert.assertEquals(new Double(1d), v.property("age6").value());
    }

}
