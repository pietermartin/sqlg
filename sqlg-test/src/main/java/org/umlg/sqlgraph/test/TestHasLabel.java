package org.umlg.sqlgraph.test;

import com.tinkerpop.gremlin.structure.Element;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/29
 * Time: 2:21 PM
 */
public class TestHasLabel extends BaseTest {

    @Test
    public void testHasLabel() {
        this.sqlGraph.addVertex(Element.LABEL, "Person");
        this.sqlGraph.addVertex(Element.LABEL, "Person");
        this.sqlGraph.addVertex(Element.LABEL, "Person");
        this.sqlGraph.addVertex(Element.LABEL, "Person");
        this.sqlGraph.addVertex(Element.LABEL, "Person");
        this.sqlGraph.addVertex(Element.LABEL, "Person");
        this.sqlGraph.addVertex(Element.LABEL, "Person");
        this.sqlGraph.addVertex(Element.LABEL, "Person");
        this.sqlGraph.tx().commit();

        Assert.assertEquals(8, this.sqlGraph.V().has(Element.LABEL, "Person").count().next(), 0);
    }

}
