package org.umlg.sqlg.test.pgvector;

import com.pgvector.PGvector;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;

public class TestPgVector extends BaseTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(TestPgVector.class);

    @Test
    public void testPgVector() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
                    put("embedding", PropertyDefinition.of(PropertyType.pgvector(3)));
                }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
        this.sqlgGraph.tx().commit();

        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
        Assert.assertEquals(4, pgvectors.size());
        for (Vertex pgvector : pgvectors) {
            PGvector embedding = pgvector.value("embedding");
            Assert.assertArrayEquals(new float[]{1, 2, 3}, embedding.toArray(), 0F);
        }
    }

    @Test
    public void testUpdatePgVector() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
                    put("embedding", PropertyDefinition.of(PropertyType.pgvector(3)));
                }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
        this.sqlgGraph.tx().commit();

        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
        Assert.assertEquals(4, pgvectors.size());
        for (Vertex pgvector : pgvectors) {
            PGvector embedding = pgvector.value("embedding");
            Assert.assertArrayEquals(new float[]{1, 2, 3}, embedding.toArray(), 0F);
            pgvector.property("embedding", new PGvector(new float[]{3, 2, 1}));
        }
        this.sqlgGraph.tx().commit();

        pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
        Assert.assertEquals(4, pgvectors.size());
        for (Vertex pgvector : pgvectors) {
            PGvector embedding = pgvector.value("embedding");
            Assert.assertArrayEquals(new float[]{3, 2, 1}, embedding.toArray(), 0F);
        }
    }

    @Test
    public void testPgVectorNormalBulkMode() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
                    put("embedding", PropertyDefinition.of(PropertyType.pgvector(3)));
                }});
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
        this.sqlgGraph.tx().commit();

        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
        Assert.assertEquals(4, pgvectors.size());
        for (Vertex pgvector : pgvectors) {
            PGvector embedding = pgvector.value("embedding");
            Assert.assertArrayEquals(new float[]{1, 2, 3}, embedding.toArray(), 0F);
        }
    }

    @Test
    public void testPgVectorStreamingBulkMode() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
                    put("embedding", PropertyDefinition.of(PropertyType.pgvector(3)));
                }});
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.streamVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
        this.sqlgGraph.streamVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
        this.sqlgGraph.streamVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
        this.sqlgGraph.streamVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
        this.sqlgGraph.tx().commit();

        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
        Assert.assertEquals(4, pgvectors.size());
        for (Vertex pgvector : pgvectors) {
            PGvector embedding = pgvector.value("embedding");
            Assert.assertArrayEquals(new float[]{1, 2, 3}, embedding.toArray(), 0F);
        }
    }


}
