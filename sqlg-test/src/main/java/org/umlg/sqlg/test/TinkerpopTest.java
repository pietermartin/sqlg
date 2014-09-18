package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import com.tinkerpop.gremlin.structure.util.batch.BatchGraph;
import com.tinkerpop.gremlin.structure.util.batch.Exists;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_INTEGER_VALUES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2014/07/13
 * Time: 6:32 PM
 */
public class TinkerpopTest extends BaseTest {

    @Test
    public void shouldLoadVerticesIncrementallyWithNamedIdentifierOverwriteExistingVertex() {
        final BatchGraph graph = BatchGraph.build(this.sqlG)
                .incrementalLoading(true, Exists.OVERWRITE, Exists.IGNORE)
                .vertexIdKey("name")
                .bufferSize(1).create();
        graph.addVertex(T.id, "marko", "age", 29);
        final Vertex v1 = graph.addVertex(T.id, "stephen", "age", 37);
        final Vertex v2 = graph.addVertex(T.id, "marko", "age", 34);
        v1.addEdge("knows", v2, "weight", 1.0d);
        graph.tx().commit();

        final Vertex vStephen = this.sqlG.V().<Vertex>has("name", "stephen").next();
        assertEquals(37, vStephen.property("age").value());
        assertEquals(new Long(1), vStephen.outE("knows").has("weight", 1.0d).inV().has("name", "marko").count().next());

        final Vertex vMarko = this.sqlG.V().<Vertex>has("name", "marko").next();
        assertEquals(2, vMarko.properties("age").count().next().intValue());
        assertEquals(2, vMarko.properties("name").count().next().intValue());
        assertTrue(vMarko.valueMap().next().get("age").contains(29));
        assertTrue(vMarko.valueMap().next().get("age").contains(34));
    }

    protected void printTraversalForm(final Traversal traversal) {
        System.out.println("Testing: " + traversal);
        traversal.strategies().apply();
        System.out.println("         " + traversal);
    }

    private static void readGraphMLIntoGraph(final Graph g) throws IOException {
        final GraphReader reader = GraphMLReader.build().create();
        try (final InputStream stream = new FileInputStream(new File("sqlg-test/src/main/resources/tinkerpop-classic.xml"))) {
            reader.readGraph(stream, g);
        }
//        try (final InputStream stream = TinkerpopTest.class.getResourceAsStream("tinkerpop-classic.xml")) {
//            reader.readGraph(stream, g);
//        }
    }

}
