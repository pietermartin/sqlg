package org.umlg.sqlg.test.schema;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;

/**
 * Date: 2014/08/13
 * Time: 10:49 AM
 */
public class TestSchema extends BaseTest {

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
        Vertex ape = this.sqlG.addVertex(Element.LABEL, "TEST_SCHEMA2.Ape", "name", "Amuz");
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
                (a, b) -> ((String) a).endsWith((String) b),
                "Person"
        ).count().next(), 0);
        Assert.assertEquals(1, john.out("friend").has("name", "Tom").count().next(), 0);
        Assert.assertEquals(2, this.sqlG.V().has(Element.LABEL, "Person").count().next(), 0);
        Assert.assertEquals(1, this.sqlG.V().has(Element.LABEL, "TEST_SCHEMA1.Person").count().next(), 0);
        Assert.assertEquals(1, this.sqlG.V().has(Element.LABEL, "TEST_SCHEMA2.Person").count().next(), 0);
        Assert.assertEquals(1, this.sqlG.V().has(Element.LABEL, "TEST_SCHEMA2.Ape").count().next(), 0);
        Assert.assertEquals(1, this.sqlG.V().has(Element.LABEL, "Ape").count().next(), 0);
    }

    @Test
    public void testManySchemas() {
        Vertex previous = null;
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 100; j++) {
                Vertex v = this.sqlG.addVertex(Element.LABEL, "Schema" + i + ".Person", "name1", "n" + j, "name2", "n" + j);
                if (previous != null) {
                    previous.addEdge("edge", v, "name1", "n" + j, "name2", "n" + j);
                }
                previous = v;
            }
        }
        this.sqlG.tx().commit();
        Assert.assertEquals(1000, this.sqlG.V().count().next(), 0);
        Assert.assertEquals(1000, this.sqlG.V().has(Element.LABEL, "Person").count().next(), 0);
        Assert.assertEquals(100, this.sqlG.V().has(Element.LABEL, "Schema5.Person").count().next(), 0);
        Assert.assertEquals(999, this.sqlG.E().count().next(), 0);
        Assert.assertEquals(999, this.sqlG.E().has(Element.LABEL, "edge").count().next(), 0);
        Assert.assertEquals(100, this.sqlG.E().has(Element.LABEL, "Schema0.edge").count().next(), 0);
        Assert.assertEquals(99, this.sqlG.E().has(Element.LABEL, "Schema9.edge").count().next(), 0);
    }

    @Test
    public void testLabelsForSchemaBeforeCommit() {
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        Assert.assertEquals(2, this.sqlG.V().has(Element.LABEL, "Person").count().next(), 0);
    }

}
