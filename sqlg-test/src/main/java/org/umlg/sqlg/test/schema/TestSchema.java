package org.umlg.sqlg.test.schema;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SchemaManager;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Date: 2014/08/13
 * Time: 10:49 AM
 */
public class TestSchema extends BaseTest {

    @Test
    public void testSchema() {
        this.sqlgGraph.addVertex(T.label, "TEST_SCHEMA1.Person", "name", "John");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
    }

    @Test
    public void testEdgeBetweenSchemas() {
        Vertex john = this.sqlgGraph.addVertex(T.label, "TEST_SCHEMA1.Person", "name", "John");
        Vertex tom = this.sqlgGraph.addVertex(T.label, "TEST_SCHEMA2.Person", "name", "Tom");
        Vertex ape = this.sqlgGraph.addVertex(T.label, "TEST_SCHEMA2.Ape", "name", "Amuz");
        john.addEdge("friend", tom);
        john.addEdge("pet", ape);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(3, this.sqlgGraph.traversal().V().count().next(), 0);
        Assert.assertEquals(1, vertexTraversal(john).out("friend").count().next(), 0);
        Assert.assertEquals(tom, vertexTraversal(john).out("friend").next());
        Assert.assertEquals(john, vertexTraversal(tom).in("friend").next());
        Assert.assertEquals(2, this.sqlgGraph.traversal().E().count().next(), 0);
        this.sqlgGraph.traversal().E().<Edge>has(T.label, "friend").forEachRemaining(
                a -> {
                    Assert.assertEquals(john, edgeTraversal(a).outV().next());
                    Assert.assertEquals(tom, edgeTraversal(a).inV().next());
                }
        );
        Assert.assertEquals(1, vertexTraversal(john).out("friend").has("name", "Tom").count().next(), 0);
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "TEST_SCHEMA1.Person").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "TEST_SCHEMA2.Person").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "TEST_SCHEMA2.Ape").count().next(), 0);
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Ape").count().next(), 0);
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
        Assert.assertEquals(1000, this.sqlgGraph.traversal().V().count().next(), 0);
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().has(T.label, "Schema5.Person").count().next(), 0);
        Assert.assertEquals(999, this.sqlgGraph.traversal().E().count().next(), 0);
        // all schemas are now taken into account (see https://github.com/pietermartin/sqlg/issues/65)
        Assert.assertEquals(999, this.sqlgGraph.traversal().E().has(T.label, "edge").count().next(), 0);
        Assert.assertEquals(100, this.sqlgGraph.traversal().E().has(T.label, "Schema0.edge").count().next(), 0);
        Assert.assertEquals(99, this.sqlgGraph.traversal().E().has(T.label, "Schema9.edge").count().next(), 0);
    }

    @Test
    public void testLabelsForSchemaBeforeCommit() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
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
        Assert.assertEquals(3, vertices.size());
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
    
    @Test
    public void testEnsureSchema(){
    	SchemaManager mgr=this.sqlgGraph.getSchemaManager();
    	this.sqlgGraph.addVertex(T.label, "A.A");
    	this.sqlgGraph.tx().commit();

    	assertTrue(mgr.schemaExist(this.sqlgGraph.getSqlDialect().getPublicSchema()));
    	assertTrue(mgr.schemaExist("A"));
    	assertTrue(mgr.ensureSchemaExists("A"));
    	
    	assertFalse(mgr.schemaExist("B"));
    	// false means didn't exist
    	assertFalse(mgr.ensureSchemaExists("B"));
    	// not committed yet
    	assertFalse(mgr.schemaExist("B"));
    	this.sqlgGraph.tx().commit();
    	assertTrue(mgr.schemaExist("B"));
    	
    }

}
