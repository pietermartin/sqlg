package org.umlg.sqlg.test.pgvector;

import com.pgvector.PGsparsevec;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;

public class TestPgSparsevec extends BaseTest {

    @Test
    public void testPgVector() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
                    put("embedding", PropertyDefinition.of(PropertyType.pgsparcvec(3)));
                }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGsparsevec(new float[]{1, 2, 3}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGsparsevec(new float[]{1, 2, 3}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGsparsevec(new float[]{1, 2, 3}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGsparsevec(new float[]{1, 2, 3}));
        this.sqlgGraph.tx().commit();

        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
        Assert.assertEquals(4, pgvectors.size());
        for (Vertex pgvector : pgvectors) {
            PGsparsevec embedding = pgvector.value("embedding");
            Assert.assertArrayEquals(new float[]{1, 2, 3}, embedding.toArray(), 0F);
        }
    }

    @Test
    public void testPgVectorNormalBulkMode() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
                    put("embedding", PropertyDefinition.of(PropertyType.pgsparcvec(3)));
                }});
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGsparsevec(new float[]{1, 2, 3}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGsparsevec(new float[]{1, 2, 3}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGsparsevec(new float[]{1, 2, 3}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGsparsevec(new float[]{1, 2, 3}));
        this.sqlgGraph.tx().commit();

        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
        Assert.assertEquals(4, pgvectors.size());
        for (Vertex pgvector : pgvectors) {
            PGsparsevec embedding = pgvector.value("embedding");
            Assert.assertArrayEquals(new float[]{1, 2, 3}, embedding.toArray(), 0F);
        }
    }

    @Test
    public void testPgVectorStreamingBulkMode() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
                    put("embedding", PropertyDefinition.of(PropertyType.pgsparcvec(3)));
                }});
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.streamVertex(T.label, "PGVector", "embedding", new PGsparsevec(new float[]{1, 2, 3}));
        this.sqlgGraph.streamVertex(T.label, "PGVector", "embedding", new PGsparsevec(new float[]{1, 2, 3}));
        this.sqlgGraph.streamVertex(T.label, "PGVector", "embedding", new PGsparsevec(new float[]{1, 2, 3}));
        this.sqlgGraph.streamVertex(T.label, "PGVector", "embedding", new PGsparsevec(new float[]{1, 2, 3}));
        this.sqlgGraph.tx().commit();

        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
        Assert.assertEquals(4, pgvectors.size());
        for (Vertex pgvector : pgvectors) {
            PGsparsevec embedding = pgvector.value("embedding");
            Assert.assertArrayEquals(new float[]{1, 2, 3}, embedding.toArray(), 0F);
        }
    }

}
