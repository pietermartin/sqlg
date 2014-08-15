package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/08/13
 * Time: 10:49 AM
 */
public class TestSchema extends  BaseTest {

//    @Test
    public void testSchema() {
        this.sqlG.addVertex(Element.LABEL, "TEST_SCHEMA1.Person", "name", "John");
        this.sqlG.tx().commit();
        Assert.assertEquals(1, this.sqlG.V().has(Element.LABEL, "Person").count().next(), 0);
    }

    @Test
    public void testEdgeBetweenSchemas() {
        Vertex john = this.sqlG.addVertex(Element.LABEL, "TEST_SCHEMA1.Person", "name", "John");
        Vertex tom = this.sqlG.addVertex(Element.LABEL, "TEST_SCHEMA2.Person", "name", "Tom");
        Vertex ape = this.sqlG.addVertex(Element.LABEL, "TEST_SCHEMA2.Ape", "name", "Zuma");
        john.addEdge("friend", tom);
        john.addEdge("pet", ape);
        this.sqlG.tx().commit();
        Assert.assertEquals(3, this.sqlG.V().count().next(), 0);
        Assert.assertEquals(1, john.out("friend").count().next(), 0);
        Assert.assertEquals(tom, john.out("friend").next());
        Assert.assertEquals(john, tom.in("friend").next());
        Assert.assertEquals(2, this.sqlG.E().count().next(), 0);
        this.sqlG.E().<Edge>has(Element.LABEL, "friend").forEach(
                a -> {
                    Assert.assertEquals(john, a.outV().next());
                    Assert.assertEquals(tom, a.inV().next());
                }
        );
        Assert.assertEquals(2, this.sqlG.V().<Vertex>has(
                Element.LABEL,
                (a,b)->((String)a).endsWith((String)b),
                "Person"
        ).count().next(), 0);
//        Assert.assertEquals(1, john.out("friend").has("name", "John").count().next(), 0);
//        Assert.assertEquals(1, this.sqlG.V().has(Element.LABEL, "TEST_SCHEMA1.Person").count().next(), 0);
    }

}
