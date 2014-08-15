package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/08/13
 * Time: 10:49 AM
 */
public class TestSchema extends  BaseTest {

    @Test
    public void testSchema() {
        this.sqlG.addVertex(Element.LABEL, "TEST_SCHEMA1.Person", "name", "John");
        this.sqlG.tx().commit();
        Assert.assertEquals(1, this.sqlG.V().has(Element.LABEL, "Person").count().next(), 0);
    }

    @Test
    public void testEdgeBetweenSchemas() {
        Vertex john = this.sqlG.addVertex(Element.LABEL, "TEST_SCHEMA1.Person", "name", "John");
        Vertex tom = this.sqlG.addVertex(Element.LABEL, "TEST_SCHEMA2.Person", "name", "Tom");
        john.addEdge("friend", tom);
        this.sqlG.tx().commit();
    }

}
