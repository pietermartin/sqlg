package org.umlg.sqlgraph.h2database.test;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.GraphManager;
import com.tinkerpop.gremlin.GraphProvider;
import com.tinkerpop.gremlin.structure.*;
import org.junit.Test;
import org.umlg.sqlgraph.structure.SqlGraph;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_STRING_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_PERSISTENCE;
import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS;
import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_PROPERTIES;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Date: 2014/07/13
 * Time: 6:32 PM
 */
public class TinkerpopTest extends BaseTest {

    /**
     * Generate a graph with lots of edges and vertices, then test vertex/edge counts on removal of each vertex.
     */
    @Test
    public void shouldRemoveVertices() {
        SqlGraph g = this.sqlGraph;
        final int vertexCount = 500;
        final List<Vertex> vertices = new ArrayList<>();
        final List<Edge> edges = new ArrayList<>();

        IntStream.range(0, vertexCount).forEach(i -> vertices.add(g.addVertex()));
        tryCommit(g, AbstractGremlinSuite.assertVertexEdgeCounts(vertexCount, 0));

        for (int i = 0; i < vertexCount; i = i + 2) {
            final Vertex a = vertices.get(i);
            final Vertex b = vertices.get(i + 1);
            edges.add(a.addEdge("a" + UUID.randomUUID(), b));
        }

        tryCommit(g, AbstractGremlinSuite.assertVertexEdgeCounts(vertexCount, vertexCount / 2));

        int counter = 0;
        for (Vertex v : vertices) {
            counter = counter + 1;
            v.remove();

            if ((counter + 1) % 2 == 0) {
                final int currentCounter = counter;
                tryCommit(g, AbstractGremlinSuite.assertVertexEdgeCounts(
                        vertexCount - currentCounter, edges.size() - ((currentCounter + 1) / 2)));
            }
        }
    }

    protected void tryCommit(final SqlGraph g, final Consumer<Graph> assertFunction) {
        assertFunction.accept(g);
        if (g.getFeatures().graph().supportsTransactions()) {
            g.tx().commit();
            assertFunction.accept(g);
        }
    }

    protected void tryCommit(final Graph g) {
        if (g.getFeatures().graph().supportsTransactions())
            g.tx().commit();
    }

    protected void tryRollback(final Graph g) {
        if (g.getFeatures().graph().supportsTransactions())
            g.tx().rollback();
    }

}
