package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.List;

public class TestQueryTimeout extends BaseTest{

    @Test
    public void testQueryTimeout() {
        Assume.assumeTrue(isPostgres());
        sqlgGraph.addVertex(T.label, "A", "name", "value-1", "name1", "value-1");
        sqlgGraph.tx().commit();
        sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 10_000_000; i++) {
            sqlgGraph.streamVertex(T.label, "A", "name", "value" + i, "name1", "value" + i);
        }
        sqlgGraph.tx().commit();

        sqlgGraph.tx().setQueryTimeout(1);
        try {
            List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("A").toList();
            Assert.assertEquals(10_000_001, vertices.size());
        } catch (RuntimeException e) {
            Assert.assertEquals("org.postgresql.util.PSQLException", e.getCause().getClass().getName());
            sqlgGraph.tx().rollback();
        }
        sqlgGraph.addVertex(T.label, "A", "name", "value-1", "name1", "value-1");
        sqlgGraph.tx().commit();
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(10_000_002, vertices.size());
    }
}
