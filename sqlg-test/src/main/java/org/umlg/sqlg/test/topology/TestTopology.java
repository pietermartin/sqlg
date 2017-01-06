package org.umlg.sqlg.test.topology;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2016/06/27
 * Time: 1:40 PM
 */
public class TestTopology extends BaseTest {

    @Test
    public void testTopologyTraversal() {
        Vertex gis = this.sqlgGraph.addVertex(T.label, "Gis", "name", "HaloGis1");
        Vertex something = this.sqlgGraph.addVertex(T.label, "Something", "name", "Something1");
        gis.addEdge("testEdge", something, "edgeProperty", "asdasd");
        this.sqlgGraph.tx().commit();
        assertEquals(2, this.sqlgGraph.topology().V().hasLabel("sqlg_schema.schema").out("schema_vertex").count().next().intValue());
        assertEquals(2, this.sqlgGraph.topology().V().hasLabel("sqlg_schema.vertex").in("schema_vertex").count().next().intValue());
        assertEquals(2, this.sqlgGraph.topology().V().hasLabel("sqlg_schema.vertex").out("vertex_property").count().next().intValue());
        assertEquals(2, this.sqlgGraph.topology().V().hasLabel("sqlg_schema.property").in("vertex_property").count().next().intValue());
        assertEquals(1, this.sqlgGraph.topology().V().hasLabel("sqlg_schema.property").in("edge_property").count().next().intValue());
  
        Vertex v= this.sqlgGraph.topology().V().hasLabel("sqlg_schema.schema").has("name","public").next();
        assertTrue(v.edges(Direction.OUT, "schema_vertex").hasNext());
        
        assertEquals(2, this.sqlgGraph.topology().V().hasLabel("sqlg_schema.schema").as("schema").select("schema").out("schema_vertex").count().next().intValue());
        assertEquals(2, this.sqlgGraph.topology().V().hasLabel("sqlg_schema.schema").as("schema").values("name").as("schemaName").select("schema").out("schema_vertex").count().next().intValue());
            
        assertEquals("testEdge",this.sqlgGraph.topology().V().hasLabel("sqlg_schema.property").in("edge_property").values("name").next());
        
    }

    
    //This test a bug in rollback on edges.
    @Test
    public void testRollback() {
        loadModern();
        final Traversal<Vertex, Edge> traversal = this.sqlgGraph.traversal().V().aggregate("x").as("a").select("x").unfold().addE("existsWith").to("a").property("time", "now");
        IteratorUtils.asList(traversal);
        this.sqlgGraph.tx().rollback();
    }

}
