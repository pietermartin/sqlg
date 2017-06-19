package org.umlg.sqlg.test.topology;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.EdgeLabel;
import org.umlg.sqlg.structure.Schema;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * test deletion behavior in a specific scenarios
 *
 * @author JP Moresmau
 */
public class TestTopologyDeleteSpecific extends BaseTest {

    /**
     * this failed with a NPE because we lost the table definition we're working on
     *
     * @throws Exception
     */
    @Test
    public void testSchemaDelete() throws Exception {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supporstDistribution());
        String schema = "willDelete";
        Vertex v1 = sqlgGraph.addVertex(T.label, schema + ".t1", "name", "n1", "hello", "world");
        sqlgGraph.tx().commit();
        Configuration c = getConfigurationClone();
        c.setProperty(SqlgGraph.DISTRIBUTED, true);
        sqlgGraph = SqlgGraph.open(c);
        sqlgGraph.getTopology().getSchema(schema).ifPresent((Schema s) -> s.remove(false));
        sqlgGraph.tx().commit();

        v1 = sqlgGraph.addVertex(T.label, schema + ".t1", "name", "n1");
        Vertex v2 = sqlgGraph.addVertex(T.label, schema + ".t2", "name", "n2");
        Edge e1 = v1.addEdge("e1", v2);
        sqlgGraph.tx().commit();

        sqlgGraph.tx().normalBatchModeOn();
        v1.property("hello", "world");
        // this line was failing
        e1.property("hello", "world");

        sqlgGraph.tx().commit();

        assertEquals("world", e1.value("hello"));
    }

    /**
     * @see <a href="https://github.com/pietermartin/sqlg/issues/212">https://github.com/pietermartin/sqlg/issues/212</a>
     */
    @Test
    public void testRemoveAndAddInSameTransaction() {
        //remove it, it does not exist but duplicating work logic.
        Optional<EdgeLabel> aaEdgeLabelOpt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "aa");
        if (aaEdgeLabelOpt.isPresent()) {
            aaEdgeLabelOpt.get().remove(false);
        }
        Optional<VertexLabel> aVertexLabelOpt = this.sqlgGraph.getTopology().getVertexLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "A");
        if (aVertexLabelOpt.isPresent()) {
            aVertexLabelOpt.get().remove(false);
        }

        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("A");
        aVertexLabel.ensureEdgeLabelExist("aa", aVertexLabel);
        this.sqlgGraph.tx().commit();

        aaEdgeLabelOpt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "aa");
        if (aaEdgeLabelOpt.isPresent()) {
            aaEdgeLabelOpt.get().remove(false);
        }
        aVertexLabelOpt = this.sqlgGraph.getTopology().getVertexLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "A");
        if (aVertexLabelOpt.isPresent()) {
            aVertexLabelOpt.get().remove(false);
        }

        aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("A");
        aVertexLabel.ensureEdgeLabelExist("aa", aVertexLabel);
        this.sqlgGraph.tx().commit();

        aaEdgeLabelOpt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "aa");
        assertTrue(aaEdgeLabelOpt.isPresent());
        aVertexLabelOpt = this.sqlgGraph.getTopology().getVertexLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "A");
        assertTrue(aVertexLabelOpt.isPresent());

    }
}
