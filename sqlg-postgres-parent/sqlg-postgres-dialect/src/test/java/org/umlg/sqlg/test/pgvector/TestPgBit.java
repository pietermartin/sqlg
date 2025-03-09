package org.umlg.sqlg.test.pgvector;

import com.pgvector.PGbit;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;

public class TestPgBit extends BaseTest {

    @Test
    public void testPgVector() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
                    put("embedding", PropertyDefinition.of(PropertyType.pgbit(3)));
                }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGbit(new boolean[]{true, false, true}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGbit(new boolean[]{true, false, true}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGbit(new boolean[]{true, false, true}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGbit(new boolean[]{true, false, true}));
        this.sqlgGraph.tx().commit();

        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
        Assert.assertEquals(4, pgvectors.size());
        for (Vertex pgvector : pgvectors) {
            PGbit embedding = pgvector.value("embedding");
            Assert.assertArrayEquals(new boolean[]{true, false, true}, embedding.toArray());
        }
    }

    @Test
    public void testPgVectorNormalBulkMode() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
                    put("embedding", PropertyDefinition.of(PropertyType.pgbit(3)));
                }});
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGbit(new boolean[]{true, false, true}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGbit(new boolean[]{true, false, true}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGbit(new boolean[]{true, false, true}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGbit(new boolean[]{true, false, true}));
        this.sqlgGraph.tx().commit();

        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
        Assert.assertEquals(4, pgvectors.size());
        for (Vertex pgvector : pgvectors) {
            PGbit embedding = pgvector.value("embedding");
            Assert.assertArrayEquals(new boolean[]{true, false, true}, embedding.toArray());
        }
    }

    @Test
    public void testPgVectorStreamingBulkMode() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
                    put("embedding", PropertyDefinition.of(PropertyType.pgbit(3)));
                }});
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.streamVertex(T.label, "PGVector", "embedding", new PGbit(new boolean[]{true, false, true}));
        this.sqlgGraph.streamVertex(T.label, "PGVector", "embedding", new PGbit(new boolean[]{true, false, true}));
        this.sqlgGraph.streamVertex(T.label, "PGVector", "embedding", new PGbit(new boolean[]{true, false, true}));
        this.sqlgGraph.streamVertex(T.label, "PGVector", "embedding", new PGbit(new boolean[]{true, false, true}));
        this.sqlgGraph.tx().commit();

        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
        Assert.assertEquals(4, pgvectors.size());
        for (Vertex pgvector : pgvectors) {
            PGbit embedding = pgvector.value("embedding");
            Assert.assertArrayEquals(new boolean[]{true, false, true}, embedding.toArray());
        }
    }

}
