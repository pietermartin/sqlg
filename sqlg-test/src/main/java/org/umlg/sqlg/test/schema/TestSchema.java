package org.umlg.sqlg.test.schema;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2014/08/13
 * Time: 10:49 AM
 */
public class TestSchema extends BaseTest {

    @Test
    public void testEdgesAcrossSchema() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C.C");
        a1.addEdge("yourEdge", b1);
        b1.addEdge("yourEdge", c1);
        this.sqlgGraph.tx().commit();
        List<Edge> edges = this.sqlgGraph.traversal().E().toList();
        assertEquals(2, edges.size());
        for (Edge edge : edges) {
            assertEquals("yourEdge", edge.label());
        }
        assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("A.yourEdge").count().next(), 0);
        assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("B.yourEdge").count().next(), 0);
    }

    @Test
    public void testSchema() {
        this.sqlgGraph.addVertex(T.label, "TEST_SCHEMA1.Person", "name", "John");
        this.sqlgGraph.tx().commit();
        assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
    }

    @Test
    public void testEdgeBetweenSchemas() {
        Vertex john = this.sqlgGraph.addVertex(T.label, "TEST_SCHEMA1.Person", "name", "John");
        Vertex tom = this.sqlgGraph.addVertex(T.label, "TEST_SCHEMA2.Person", "name", "Tom");
        Vertex ape = this.sqlgGraph.addVertex(T.label, "TEST_SCHEMA2.Ape", "name", "Amuz");
        john.addEdge("friend", tom);
        john.addEdge("pet", ape);
        this.sqlgGraph.tx().commit();
        assertEquals(3, this.sqlgGraph.traversal().V().count().next(), 0);
        assertEquals(1, vertexTraversal(john).out("friend").count().next(), 0);
        assertEquals(tom, vertexTraversal(john).out("friend").next());
        assertEquals(john, vertexTraversal(tom).in("friend").next());
        assertEquals(2, this.sqlgGraph.traversal().E().count().next(), 0);
        this.sqlgGraph.traversal().E().<Edge>has(T.label, "friend").forEachRemaining(
                a -> {
                    assertEquals(john, edgeTraversal(a).outV().next());
                    assertEquals(tom, edgeTraversal(a).inV().next());
                }
        );
        assertEquals(1, vertexTraversal(john).out("friend").has("name", "Tom").count().next(), 0);
        assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
        assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "TEST_SCHEMA1.Person").count().next(), 0);
        assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "TEST_SCHEMA2.Person").count().next(), 0);
        assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "TEST_SCHEMA2.Ape").count().next(), 0);
        assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Ape").count().next(), 0);
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
        assertEquals(1000, this.sqlgGraph.traversal().V().count().next(), 0);
        assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
        assertEquals(100, this.sqlgGraph.traversal().V().has(T.label, "Schema5.Person").count().next(), 0);
        assertEquals(999, this.sqlgGraph.traversal().E().count().next(), 0);
        // all schemas are now taken into account (see https://github.com/pietermartin/sqlg/issues/65)
        assertEquals(999, this.sqlgGraph.traversal().E().has(T.label, "edge").count().next(), 0);
        assertEquals(100, this.sqlgGraph.traversal().E().has(T.label, "Schema0.edge").count().next(), 0);
        assertEquals(99, this.sqlgGraph.traversal().E().has(T.label, "Schema9.edge").count().next(), 0);
    }

    @Test
    public void testLabelsForSchemaBeforeCommit() {
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        assertEquals(2, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
    }

    @Test
    public void testGetAllTableLabels() {
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex address = this.sqlgGraph.addVertex(T.label, "Address");
        person.addEdge("person_address", address);
        this.sqlgGraph.tx().commit();

        Assert.assertTrue(this.sqlgGraph.getSchemaManager().getTableLabels(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person"))!=null);

        Pair<Set<SchemaTable>, Set<SchemaTable>> labels = this.sqlgGraph.getSchemaManager().getTableLabels(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person"));
        Assert.assertTrue(labels.getRight().contains(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "E_person_address")));

        Map<String, Set<String>> edgeForeignKeys = this.sqlgGraph.getSchemaManager().getAllEdgeForeignKeys();
        Assert.assertTrue(edgeForeignKeys.containsKey(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "E_person_address").toString()));

        Vertex car = this.sqlgGraph.addVertex(T.label, "Car");
        person.addEdge("drives", car);

        Vertex pet = this.sqlgGraph.addVertex(T.label, "Pet");
        person.addEdge("person_address", pet);

        labels = this.sqlgGraph.getSchemaManager().getTableLabels(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person"));
        Assert.assertTrue(labels.getRight().contains(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "E_person_address")));

        edgeForeignKeys = this.sqlgGraph.getSchemaManager().getAllEdgeForeignKeys();
        Assert.assertTrue(edgeForeignKeys.containsKey(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "E_person_address").toString()));
    }

    @Test
    public void testSchemaPropertyEndingIn_ID() {
        this.sqlgGraph.addVertex(T.label, "A", "TRX Group ID", 1234);
        this.sqlgGraph.addVertex(T.label, "A", "TRX Group ID", 1234);
        this.sqlgGraph.addVertex(T.label, "A", "TRX Group ID", 1234);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices =  this.sqlgGraph.traversal().V().hasLabel("A").toList();
        assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.get(0).property("TRX Group ID").isPresent());
        Assert.assertTrue(vertices.get(1).property("TRX Group ID").isPresent());
        Assert.assertTrue(vertices.get(2).property("TRX Group ID").isPresent());
    }

    @Test
    public void testUnprefixedEdgeLabel() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B");
        a1.addEdge("eee", b1);
        this.sqlgGraph.tx().commit();
        assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("eee").count().next().intValue());
        assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("A.eee").count().next().intValue());
        assertEquals(0, this.sqlgGraph.traversal().E().hasLabel("B.eee").count().next().intValue());
        assertEquals(0, this.sqlgGraph.traversal().E().hasLabel("public.eee").count().next().intValue());
    }

}
