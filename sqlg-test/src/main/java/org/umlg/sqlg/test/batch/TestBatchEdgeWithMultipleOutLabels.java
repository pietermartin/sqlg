package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/08/09
 * Time: 9:10 AM
 */
public class TestBatchEdgeWithMultipleOutLabels extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testBatchModeEdgeMultipleOutLabels() throws InterruptedException {
        sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 5; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A1");
            Vertex a2  = this.sqlgGraph.addVertex(T.label, "A2");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B");
            a1.addEdge("address", b);
            a2.addEdge("address", b);
        }
        this.sqlgGraph.tx().commit();
        assertEquals(5, this.sqlgGraph.traversal().V().hasLabel("A1").count().next().intValue());
        assertEquals(5, this.sqlgGraph.traversal().V().hasLabel("A2").count().next().intValue());
        assertEquals(5, this.sqlgGraph.traversal().V().hasLabel("A1").out("address").count().next().intValue());
        assertEquals(5, this.sqlgGraph.traversal().V().hasLabel("A2").out("address").count().next().intValue());

        sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 5; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A1");
            Vertex a2  = this.sqlgGraph.addVertex(T.label, "A2");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B");
            a1.addEdge("address", b);
            a2.addEdge("address", b);
        }
        this.sqlgGraph.tx().commit();
        testBatchModeEdgeMulitpleOutLabels_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchModeEdgeMulitpleOutLabels_assert(this.sqlgGraph1);
        }
    }

    private void testBatchModeEdgeMulitpleOutLabels_assert(SqlgGraph sqlgGraph) {
        assertEquals(10, sqlgGraph.traversal().V().hasLabel("A1").count().next().intValue());
        assertEquals(10, sqlgGraph.traversal().V().hasLabel("A2").count().next().intValue());
        assertEquals(10, sqlgGraph.traversal().V().hasLabel("A1").out("address").count().next().intValue());
        assertEquals(10, sqlgGraph.traversal().V().hasLabel("A2").out("address").count().next().intValue());
    }

    @Test
    public void issue57() throws InterruptedException {
        sqlgGraph.tx().normalBatchModeOn();
        String key = "AbstractTinkerPopFhirGraph.Prop.ID";
        int indexCount = 100;
        for (int index = 0; index < indexCount; index++) {
            Vertex v1 = sqlgGraph.addVertex(T.label, "Patient", key, index);
            Vertex v2 = sqlgGraph.addVertex(T.label, "HumanNameDt", key, UUID.randomUUID().toString());
            v1.addEdge("name", v2);

            Vertex v3 = sqlgGraph.addVertex(T.label, "Condition", key, index);
            Vertex v4 = sqlgGraph.addVertex(T.label, "CodeableConceptDt", key, UUID.randomUUID().toString());
            Vertex v5 = sqlgGraph.addVertex(T.label, "CodingDt", key, UUID.randomUUID().toString());
            v3.addEdge("code", v4);
            v4.addEdge("coding", v5);

            v3.addEdge("patient", v1);

            Vertex v6 = sqlgGraph.addVertex(T.label, "CodeableConcept", key, UUID.randomUUID().toString());
            v3.addEdge("category", v6);

            Vertex v7 = sqlgGraph.addVertex(T.label, "CodingDt", key, UUID.randomUUID().toString());
            v6.addEdge("coding", v7);
        }
        sqlgGraph.tx().commit();

        testIssue57_assert(this.sqlgGraph, indexCount);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testIssue57_assert(this.sqlgGraph1, indexCount);
        }

    }

    private void testIssue57_assert(SqlgGraph sqlgGraph, int indexCount) {
        assertEquals(indexCount, sqlgGraph.traversal().V().hasLabel("Patient").count().next().intValue());
        assertEquals(indexCount, sqlgGraph.traversal().V().hasLabel("HumanNameDt").count().next().intValue());
        assertEquals(indexCount, sqlgGraph.traversal().V().hasLabel("Patient").out("name").count().next().intValue());
        sqlgGraph.traversal().V().hasLabel("Patient").out("name").forEachRemaining(v -> assertEquals("HumanNameDt", v.label()));

        assertEquals(indexCount, sqlgGraph.traversal().V().hasLabel("Condition").count().next().intValue());
        assertEquals(indexCount, sqlgGraph.traversal().V().hasLabel("CodeableConceptDt").count().next().intValue());
        assertEquals(indexCount * 2, sqlgGraph.traversal().V().hasLabel("CodingDt").count().next().intValue());
        assertEquals(indexCount, sqlgGraph.traversal().V().hasLabel("Condition").out("code").count().next().intValue());
        assertEquals(indexCount, sqlgGraph.traversal().V().hasLabel("CodeableConceptDt").out("coding").count().next().intValue());

        sqlgGraph.traversal().V().hasLabel("Condition").out("code").forEachRemaining(v -> assertEquals("CodeableConceptDt", v.label()));
        sqlgGraph.traversal().V().hasLabel("Condition").out("coding").forEachRemaining(v -> assertEquals("CodingDt", v.label()));
        sqlgGraph.traversal().V().hasLabel("Condition").out("patient").forEachRemaining(v -> assertEquals("Patient", v.label()));

        assertEquals(indexCount, sqlgGraph.traversal().V().hasLabel("CodeableConcept").count().next().intValue());
        assertEquals(indexCount, sqlgGraph.traversal().V().hasLabel("Condition").out("category").count().next().intValue());
        sqlgGraph.traversal().V().hasLabel("Condition").out("category").forEachRemaining(v -> assertEquals("CodeableConcept", v.label()));

        assertEquals(indexCount, sqlgGraph.traversal().V().hasLabel("CodeableConcept").out("coding").count().next().intValue());
        sqlgGraph.traversal().V().hasLabel("CodeableConcept").out("coding").forEachRemaining(v -> assertEquals("CodingDt", v.label()));
    }


}
