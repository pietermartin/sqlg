package org.umlg.sqlg.test.schema;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;
import static org.umlg.sqlg.structure.topology.Topology.SQLG_SCHEMA;

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

        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> localTabels = this.sqlgGraph.getTopology().getTableLabels();
        assertTrue(localTabels.containsKey(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person")));
        assertTrue(localTabels.containsKey(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Car")));
        assertTrue(localTabels.containsKey(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Model")));
        Pair<Set<SchemaTable>, Set<SchemaTable>> person = localTabels.get(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person"));
        assertEquals(0, person.getLeft().size());
        assertEquals(1, person.getRight().size());
        assertEquals(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".E_drives", person.getRight().iterator().next().toString());
        Pair<Set<SchemaTable>, Set<SchemaTable>> car = localTabels.get(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Car"));
        assertEquals(1, car.getLeft().size());
        assertEquals(1, car.getRight().size());
        assertEquals(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".E_drives", car.getLeft().iterator().next().toString());

        Pair<Set<SchemaTable>, Set<SchemaTable>> model = localTabels.get(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Model"));
        assertEquals(1, model.getLeft().size());
        assertEquals(0, model.getRight().size());
        assertEquals(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".E_model", model.getLeft().iterator().next().toString());
    }

    @Test
    public void testCaptureSchemaTableLabelsRollback() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsTransactionalSchema());
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
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> localTables = this.sqlgGraph.getTopology().getTableLabels();
        assertTrue(localTables.containsKey(SchemaTable.of(SQLG_SCHEMA, "V_vertex")));
        assertFalse(localTables.containsKey(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person")));
        assertFalse(localTables.containsKey(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Car")));
        assertFalse(localTables.containsKey(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Model")));
        assertFalse(localTables.containsKey(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "E_drives")));
        assertFalse(localTables.containsKey(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "E_model")));
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
        try (SqlgGraph sqlgGraph = SqlgGraph.open(configuration)) {
            Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> localTabels = sqlgGraph.getTopology().getTableLabels();
            assertTrue(localTabels.containsKey(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person")));
            assertTrue(localTabels.containsKey(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "V_Car")));
            assertTrue(localTabels.containsKey(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "V_Model")));
            Pair<Set<SchemaTable>, Set<SchemaTable>> person = localTabels.get(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person"));
            assertEquals(0, person.getLeft().size());
            assertEquals(1, person.getRight().size());
            assertEquals(sqlgGraph.getSqlDialect().getPublicSchema() + ".E_drives", person.getRight().iterator().next().toString());
            Pair<Set<SchemaTable>, Set<SchemaTable>> car = localTabels.get(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "V_Car"));
            assertEquals(1, car.getLeft().size());
            assertEquals(1, car.getRight().size());
            assertEquals(sqlgGraph.getSqlDialect().getPublicSchema() + ".E_drives", car.getLeft().iterator().next().toString());

            Pair<Set<SchemaTable>, Set<SchemaTable>> model = localTabels.get(SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), "V_Model"));
            assertEquals(1, model.getLeft().size());
            assertEquals(0, model.getRight().size());
            assertEquals(sqlgGraph.getSqlDialect().getPublicSchema() + ".E_model", model.getLeft().iterator().next().toString());
        }
    }
}
