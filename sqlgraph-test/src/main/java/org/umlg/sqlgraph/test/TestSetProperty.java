package org.umlg.sqlgraph.test;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 7:48 PM
 */
public class TestSetProperty extends BaseTest {

    @Test
    public void testSetProperty() {
        Vertex marko = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "marko");
        marko.property("surname", "xxxx");
        this.sqlGraph.tx().commit();
        Assert.assertEquals("xxxx", marko.property("surname").value());
    }

    @Test
    public void testPropertyManyTimes() {
        Vertex v = this.sqlGraph.addVertex("age", 1, "name", "marko", "name", "john");
        this.sqlGraph.tx().commit();
    }
}
