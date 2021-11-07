package org.umlg.sqlg.test.topology;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.Optional;

import static org.junit.Assert.*;

/**
 * test deletion behavior in a specific scenarios
 *
 * @author JP Moresmau
 */
public class TestTopologyDeleteSpecific extends BaseTest {

    /**
     * @see <a href="https://github.com/pietermartin/sqlg/issues/306">https://github.com/pietermartin/sqlg/issues/306</a>
     */
    @Test
    public void testDeleteSchemaWithEdgeRoleAcrossMultipleSchemas() {
        //Schema deletion does not work on all databases.
        Assume.assumeTrue(isPostgres());
        SqlgGraph g = this.sqlgGraph;
        Vertex a1 = g.addVertex(T.label, "A.A");
        Vertex b1 = g.addVertex(T.label, "B.B");
        a1.addEdge("ab", b1);
        g.tx().commit();

        Vertex c1 = g.addVertex(T.label, "A.C");
        c1.addEdge("ab", a1);
        g.tx().commit();

        g.getTopology().getSchema("A").ifPresent((Schema s) -> s.remove(false));
        g.tx().commit();

        assertFalse(g.getTopology().getSchema("A").isPresent());
        assertEquals(1, g.traversal().V().count().next().intValue());
        assertEquals(0, g.traversal().E().count().next().intValue());
    }

    /**
     * this failed with a NPE because we lost the table definition we're working on
     */
    @Test
    public void testSchemaDelete() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDistribution());
        String schema = "willDelete";
        sqlgGraph.addVertex(T.label, schema + ".t1", "name", "n1", "hello", "world");
        sqlgGraph.tx().commit();
        Configuration c = getConfigurationClone();
        c.setProperty(SqlgGraph.DISTRIBUTED, true);
        sqlgGraph = SqlgGraph.open(c);
        sqlgGraph.getTopology().getSchema(schema).ifPresent((Schema s) -> s.remove(false));
        sqlgGraph.tx().commit();

        Vertex v1 = sqlgGraph.addVertex(T.label, schema + ".t1", "name", "n1");
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
        aaEdgeLabelOpt.ifPresent(edgeLabel -> edgeLabel.remove(false));
        Optional<VertexLabel> aVertexLabelOpt = this.sqlgGraph.getTopology().getVertexLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "A");
        aVertexLabelOpt.ifPresent(vertexLabel -> vertexLabel.remove(false));

        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("A");
        aVertexLabel.ensureEdgeLabelExist("aa", aVertexLabel);
        this.sqlgGraph.tx().commit();

        aaEdgeLabelOpt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "aa");
        aaEdgeLabelOpt.ifPresent(edgeLabel -> edgeLabel.remove(false));
        aVertexLabelOpt = this.sqlgGraph.getTopology().getVertexLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "A");
        aVertexLabelOpt.ifPresent(vertexLabel -> vertexLabel.remove(false));

        aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("A");
        aVertexLabel.ensureEdgeLabelExist("aa", aVertexLabel);
        this.sqlgGraph.tx().commit();

        aaEdgeLabelOpt = this.sqlgGraph.getTopology().getEdgeLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "aa");
        assertTrue(aaEdgeLabelOpt.isPresent());
        aVertexLabelOpt = this.sqlgGraph.getTopology().getVertexLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "A");
        assertTrue(aVertexLabelOpt.isPresent());

    }
    
    @Test
    public void testRemoveSchemaWithCrossEdges() {
    	  Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsDistribution());
    	  Configuration c = getConfigurationClone();
          c.setProperty(SqlgGraph.DISTRIBUTED, true);
          sqlgGraph = SqlgGraph.open(c);
          
          String schema1 = "willDelete1";
          Vertex v1 = sqlgGraph.addVertex(T.label, schema1 + ".t1", "name", "n1", "hello", "world");
          String schema2 = "willDelete2";
          Vertex v2 = sqlgGraph.addVertex(T.label, schema2 + ".t2", "name", "n2", "hello", "world");
          Vertex v3 = sqlgGraph.addVertex(T.label, schema2 + ".t3", "name", "n3", "hello", "world");
          Vertex v4 = sqlgGraph.addVertex(T.label, schema2 + ".t4", "name", "n4", "hello", "world");
          
          v1.addEdge("e1", v3, "me","again");
          v2.addEdge("e1", v3, "me","again");
          v1.addEdge("e1", v4, "me","again");
          v2.addEdge("e1", v4, "me","again");
          
          
          sqlgGraph.tx().commit();
          
          assertTrue(sqlgGraph.getTopology().getSchema(schema1).isPresent());
          assertTrue(sqlgGraph.getTopology().getSchema(schema2).isPresent());
          
          sqlgGraph.getTopology().getSchema(schema1).ifPresent((Schema s) -> s.remove(false));
          sqlgGraph.tx().commit();
          
          assertFalse(sqlgGraph.getTopology().getSchema(schema1).isPresent());
          // this used to fail
          sqlgGraph.getTopology().getSchema(schema2).ifPresent((Schema s) -> s.remove(false));
          sqlgGraph.tx().commit();
        
          assertFalse(sqlgGraph.getTopology().getSchema(schema2).isPresent());
          
          
    }
}
