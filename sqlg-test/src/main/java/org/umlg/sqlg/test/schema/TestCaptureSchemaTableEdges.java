package org.umlg.sqlg.test.schema;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.util.Map;
import java.util.Set;

/**
 * Date: 2015/01/02
 * Time: 8:13 PM
 */
public class TestCaptureSchemaTableEdges extends BaseTest {

    @Test
    public void testCaptureSchemaTableLabels() {
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pieter");
        Vertex car1 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw");
        person1.addEdge("drives", car1);
        Vertex car2 = this.sqlgGraph.addVertex(T.label, "Car", "name", "toyota");
        person1.addEdge("drives", car2);
        Vertex bmw = this.sqlgGraph.addVertex(T.label, "Model", "name", "bmw");
        car1.addEdge("model", bmw);
        Vertex toyota = this.sqlgGraph.addVertex(T.label, "Model", "name", "toyota");
        car2.addEdge("model", toyota);
        this.sqlgGraph.tx().commit();

        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> localTabels = this.sqlgGraph.getSchemaManager().getLocalTableLabels();
        Assert.assertTrue(localTabels.containsKey(SchemaTable.of("public", "V_Person")));
        Assert.assertTrue(localTabels.containsKey(SchemaTable.of("public", "V_Car")));
        Assert.assertTrue(localTabels.containsKey(SchemaTable.of("public", "V_Model")));
        Pair<Set<SchemaTable>, Set<SchemaTable>> person = localTabels.get(SchemaTable.of("public", "V_Person"));
        Assert.assertEquals(0, person.getLeft().size());
        Assert.assertEquals(1, person.getRight().size());
        Assert.assertEquals("public.E_drives", person.getRight().iterator().next().toString());
        Pair<Set<SchemaTable>, Set<SchemaTable>> car = localTabels.get(SchemaTable.of("public", "V_Car"));
        Assert.assertEquals(1, car.getLeft().size());
        Assert.assertEquals(1, car.getRight().size());
        Assert.assertEquals("public.E_drives", car.getLeft().iterator().next().toString());

        Pair<Set<SchemaTable>, Set<SchemaTable>> model = localTabels.get(SchemaTable.of("public", "V_Model"));
        Assert.assertEquals(1, model.getLeft().size());
        Assert.assertEquals(0, model.getRight().size());
        Assert.assertEquals("public.E_model", model.getLeft().iterator().next().toString());
    }

    @Test
    public void testCaptureSchemaTableLablesRollback() {
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pieter");
        Vertex car1 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw");
        person1.addEdge("drives", car1);
        Vertex car2 = this.sqlgGraph.addVertex(T.label, "Car", "name", "toyota");
        person1.addEdge("drives", car2);
        Vertex bmw = this.sqlgGraph.addVertex(T.label, "Model", "name", "bmw");
        car1.addEdge("model", bmw);
        Vertex toyota = this.sqlgGraph.addVertex(T.label, "Model", "name", "toyota");
        car2.addEdge("model", toyota);
        this.sqlgGraph.tx().rollback();
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> localTables = this.sqlgGraph.getSchemaManager().getLocalTableLabels();
        Assert.assertTrue(localTables.isEmpty());
    }

    @Test
    public void testLoadTableLabels() throws Exception {
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pieter");
        Vertex car1 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw");
        person1.addEdge("drives", car1);
        Vertex car2 = this.sqlgGraph.addVertex(T.label, "Car", "name", "toyota");
        person1.addEdge("drives", car2);
        Vertex bmw = this.sqlgGraph.addVertex(T.label, "Model", "name", "bmw");
        car1.addEdge("model", bmw);
        Vertex toyota = this.sqlgGraph.addVertex(T.label, "Model", "name", "toyota");
        car2.addEdge("model", toyota);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);

        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> localTabels = this.sqlgGraph.getSchemaManager().getLocalTableLabels();
        Assert.assertTrue(localTabels.containsKey(SchemaTable.of("public", "V_Person")));
        Assert.assertTrue(localTabels.containsKey(SchemaTable.of("public", "V_Car")));
        Assert.assertTrue(localTabels.containsKey(SchemaTable.of("public", "V_Model")));
        Pair<Set<SchemaTable>, Set<SchemaTable>> person = localTabels.get(SchemaTable.of("public", "V_Person"));
        Assert.assertEquals(0, person.getLeft().size());
        Assert.assertEquals(1, person.getRight().size());
        Assert.assertEquals("public.drives", person.getRight().iterator().next().toString());
        Pair<Set<SchemaTable>, Set<SchemaTable>> car = localTabels.get(SchemaTable.of("public", "V_Car"));
        Assert.assertEquals(1, car.getLeft().size());
        Assert.assertEquals(1, car.getRight().size());
        Assert.assertEquals("public.drives", car.getLeft().iterator().next().toString());

        Pair<Set<SchemaTable>, Set<SchemaTable>> model = localTabels.get(SchemaTable.of("public", "V_Model"));
        Assert.assertEquals(1, model.getLeft().size());
        Assert.assertEquals(0, model.getRight().size());
        Assert.assertEquals("public.model", model.getLeft().iterator().next().toString());
    }
}
