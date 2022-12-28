package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.test.BaseTest;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;

/**
 * This test checks that streaming mode works with string literals
 */
public class TestStreamingFromCsvImport extends BaseTest {

    @Test
    public void testStreamCsvEdge() {
        SqlgVertex v1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex v2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        v1.streamEdge("a", v2, new LinkedHashMap<>() {{
            put("a", "a1");
            put("b", "1");
            put("c", 1.1);
            put("d", LocalDateTime.now());
        }});
        v1.streamEdge("a", v2, new LinkedHashMap<>() {{
            put("a", "a1");
            put("b", "1");
            put("c", "2.2");
            put("d", LocalDateTime.now().toString());
        }});
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V(v1).out().count().next(), 0);
    }

    @Test
    public void testStreamVertexLabelCsv() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Test",
                new LinkedHashMap<>() {{
                    put("a", PropertyDefinition.of(PropertyType.STRING));
                    put("b", PropertyDefinition.of(PropertyType.INTEGER));
                    put("c", PropertyDefinition.of(PropertyType.DOUBLE));
                }}
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.streamVertex("Test", new LinkedHashMap<>() {{
            put("a", "a1");
            put("b", "1");
            put("c", 1.1);
        }});
        this.sqlgGraph.streamVertex("Test", new LinkedHashMap<>() {{
            put("a", "a1");
            put("b", "1");
            put("c", "2.2");
        }});
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().count().next(), 0);
    }
}
