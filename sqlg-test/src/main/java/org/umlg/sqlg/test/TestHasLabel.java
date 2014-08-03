package org.umlg.sqlg.test;

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
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.tx().commit();

        Assert.assertEquals(8, this.sqlG.V().has(Element.LABEL, "Person").count().next(), 0);
    }

}
