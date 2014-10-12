package org.umlg.sqlg.test.schema;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/08/13
 * Time: 10:49 AM
 */
public class TestSchema extends BaseTest {

    @Test
    public void testSchema() {
        this.sqlgGraph.addVertex(T.label, "TEST_SCHEMA1.Person", "name", "John");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.V().has(T.label, "Person").count().next(), 0);
    }

    @Test
    public void testEdgeBetweenSchemas() {
        Vertex john = this.sqlgGraph.addVertex(T.label, "TEST_SCHEMA1.Person", "name", "John");
        Vertex tom = this.sqlgGraph.addVertex(T.label, "TEST_SCHEMA2.Person", "name", "Tom");
        Vertex ape = this.sqlgGraph.addVertex(T.label, "TEST_SCHEMA2.Ape", "name", "Amuz");
        john.addEdge("friend", tom);
        john.addEdge("pet", ape);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(3, this.sqlgGraph.V().count().next(), 0);
        Assert.assertEquals(1, john.out("friend").count().next(), 0);
        Assert.assertEquals(tom, john.out("friend").next());
        Assert.assertEquals(john, tom.in("friend").next());
        Assert.assertEquals(2, this.sqlgGraph.E().count().next(), 0);
        this.sqlgGraph.E().<Edge>has(T.label, "friend").forEach(
                a -> {
                    Assert.assertEquals(john, a.outV().next());
                    Assert.assertEquals(tom, a.inV().next());
                }
        );
        Assert.assertEquals(2, this.sqlgGraph.V().<Vertex>has(
                T.label,
                (a, b) -> ((String) a).endsWith((String) b),
                "Person"
        ).count().next(), 0);
        Assert.assertEquals(1, john.out("friend").has("name", "Tom").count().next(), 0);
        Assert.assertEquals(2, this.sqlgGraph.V().has(T.label, "Person").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.V().has(T.label, "TEST_SCHEMA1.Person").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.V().has(T.label, "TEST_SCHEMA2.Person").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.V().has(T.label, "TEST_SCHEMA2.Ape").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.V().has(T.label, "Ape").count().next(), 0);
    }

    @Test
    public void testManySchemas() {
        Vertex previous = null;
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 100; j++) {
                Vertex v = this.sqlgGraph.addVertex(T.label, "Schema" + i + ".Person", "name1", "n" + j, "name2", "n" + j);
                if (previous != null) {
                    previous.addEdge("edge", v, "name1", "n" + j, "name2", "n" + j);
                }
                previous = v;
            }
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1000, this.sqlgGraph.V().count().next(), 0);
        Assert.assertEquals(1000, this.sqlgGraph.V().has(T.label, "Person").count().next(), 0);
        Assert.assertEquals(100, this.sqlgGraph.V().has(T.label, "Schema5.Person").count().next(), 0);
        Assert.assertEquals(999, this.sqlgGraph.E().count().next(), 0);
        Assert.assertEquals(999, this.sqlgGraph.E().has(T.label, "edge").count().next(), 0);
        Assert.assertEquals(100, this.sqlgGraph.E().has(T.label, "Schema0.edge").count().next(), 0);
        Assert.assertEquals(99, this.sqlgGraph.E().has(T.label, "Schema9.edge").count().next(), 0);
    }

    @Test
    public void testLabelsForSchemaBeforeCommit() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Assert.assertEquals(2, this.sqlgGraph.V().has(T.label, "Person").count().next(), 0);
    }

}
