package org.umlg.sqlg.test.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.umlg.sqlg.structure.SchemaManager.VERTEX_PREFIX;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_EDGE_LABEL_NAME;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_EDGE_PROPERTIES_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_OUT_EDGES_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_SCHEMA;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.EdgeLabel;
import org.umlg.sqlg.structure.PropertyColumn;
import org.umlg.sqlg.structure.SchemaManager;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.Topology;
import org.umlg.sqlg.structure.TopologyChangeAction;
import org.umlg.sqlg.structure.VertexLabel;
import org.umlg.sqlg.test.BaseTest;
import org.umlg.sqlg.test.topology.TestTopologyChangeListener.TopologyListenerTest;

/**
 * Test the deletion of topology items
 * @author jpmoresmau
 *
 */
public class TestTopologyDelete extends BaseTest {

	public TestTopologyDelete() {

	}
	
	@SuppressWarnings("Duplicates")
    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
            Assume.assumeTrue(configuration.getString("jdbc.url").contains("postgresql"));
            configuration.addProperty("distributed", true);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }
	
	
	private boolean columnExistsInSQL(String schema,String table,String column) throws SQLException {
		try (ResultSet rs=this.sqlgGraph.tx().getConnection().getMetaData().getColumns(null, schema, table, column)){
			return rs.next();
		}
		
	}
	
	private void checkPropertyExistenceBeforeDeletion(String schema) throws Exception {
		Optional<VertexLabel> olbl=this.sqlgGraph.getTopology().getVertexLabel(schema, "A");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		assertTrue(olbl.get().getProperty("p1").isPresent());
		assertTrue(olbl.get().getProperty("p2").isPresent());
		
		assertEquals(1L,this.sqlgGraph.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
			.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
			.out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE).has(Topology.SQLG_SCHEMA_PROPERTY_NAME,"p1").count().next().longValue());
			
		assertEquals(1L,this.sqlgGraph.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
				.out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE).has(Topology.SQLG_SCHEMA_PROPERTY_NAME,"p2").count().next().longValue());
			
		assertTrue(columnExistsInSQL(schema, VERTEX_PREFIX+"A", "p1"));
		assertTrue(columnExistsInSQL(schema, VERTEX_PREFIX+"A", "p2"));
		
	}
	
	private void checkPropertyExistenceAfterDeletion(SqlgGraph g,String schema) throws Exception {
		Optional<VertexLabel> olbl=g.getTopology().getVertexLabel(schema, "A");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		assertFalse(olbl.get().getProperty("p1").isPresent());
		assertFalse(olbl.get().getProperty("p2").isPresent());
		
		assertEquals(0L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
			.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
			.out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE).has(Topology.SQLG_SCHEMA_PROPERTY_NAME,"p1").count().next().longValue());
			
		assertEquals(0L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
				.out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE).has(Topology.SQLG_SCHEMA_PROPERTY_NAME,"p2").count().next().longValue());
			
		assertFalse(columnExistsInSQL(schema, VERTEX_PREFIX+"A", "p1"));
		assertTrue(columnExistsInSQL(schema, VERTEX_PREFIX+"A", "p2"));
		
	}
	
	
	@Test
	public void testDeleteVertexPropertySchema() throws Exception {
		testDeleteVertexProperty("MySchema");
	}
	
	@Test
	public void testDeleteVertexPropertyNoSchema() throws Exception {
		testDeleteVertexProperty(this.sqlgGraph.getSqlDialect().getPublicSchema());
	}
	
	private void testDeleteVertexProperty(String schema) throws Exception {
		try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
			String fullLabel=schema.equals(this.sqlgGraph.getSqlDialect().getPublicSchema())?"A":schema+".A";
			Vertex a1 = this.sqlgGraph.addVertex(T.label, fullLabel, "name", "A","p1","val1","p2","val2");
			
			assertTrue(a1.property("p1").isPresent());
			assertTrue(a1.property("p2").isPresent());
			checkPropertyExistenceBeforeDeletion(schema);
			this.sqlgGraph.tx().commit();
			
			Object aid=a1.id();
			
			checkPropertyExistenceBeforeDeletion(schema);
			a1=this.sqlgGraph.traversal().V(aid).next();
			assertTrue(a1.property("p1").isPresent());
			assertTrue(a1.property("p2").isPresent());
			
			TopologyListenerTest tlt=new TopologyListenerTest();
			this.sqlgGraph.getTopology().registerListener(tlt);
			
			VertexLabel lbl=this.sqlgGraph.getTopology().getVertexLabel(schema, "A").get();
			PropertyColumn p1=lbl.getProperty("p1").get();
			p1.remove(false);
			PropertyColumn p2=lbl.getProperty("p2").get();
			p2.remove(true);
			
			assertTrue(tlt.receivedEvent(p1, TopologyChangeAction.DELETE));
			assertTrue(tlt.receivedEvent(p2, TopologyChangeAction.DELETE));
			
			checkPropertyExistenceAfterDeletion(this.sqlgGraph,schema);
			/* doesn't work because the topology allTableCache is only updated after commit
			a1=this.sqlgGraph.traversal().V(aid).next();
			assertFalse(a1.property("p1").isPresent());
			assertFalse(a1.property("p2").isPresent());
			*/
			this.sqlgGraph.tx().commit();
			checkPropertyExistenceAfterDeletion(this.sqlgGraph,schema);
			
			a1=this.sqlgGraph.traversal().V(aid).next();
			assertFalse(a1.property("p1").isPresent());
			assertFalse(a1.property("p2").isPresent());
		
			Thread.sleep(1_000);
			checkPropertyExistenceAfterDeletion(sqlgGraph1,schema);
		}
	}
	
	private void checkEdgePropertyExistenceBeforeDeletion(String schema) throws Exception {
		Optional<EdgeLabel> olbl=this.sqlgGraph.getTopology().getEdgeLabel(schema, "E");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		assertTrue(olbl.get().getProperty("p1").isPresent());
		assertTrue(olbl.get().getProperty("p2").isPresent());
		
		assertEquals(1L,this.sqlgGraph.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
			.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
			.out(SQLG_SCHEMA_OUT_EDGES_EDGE).has(SQLG_SCHEMA_EDGE_LABEL_NAME,"E")
			.out(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE).has(Topology.SQLG_SCHEMA_PROPERTY_NAME,"p1").count().next().longValue());
			
		assertEquals(1L,this.sqlgGraph.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
				.out(SQLG_SCHEMA_OUT_EDGES_EDGE).has(SQLG_SCHEMA_EDGE_LABEL_NAME,"E")
				.out(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE).has(Topology.SQLG_SCHEMA_PROPERTY_NAME,"p2").count().next().longValue());
			
		assertTrue(columnExistsInSQL(schema, SchemaManager.EDGE_PREFIX+"E", "p1"));
		assertTrue(columnExistsInSQL(schema, SchemaManager.EDGE_PREFIX+"E", "p2"));
		
	}
	
	private void checkEdgePropertyExistenceAfterDeletion(SqlgGraph g,String schema) throws Exception {
		Optional<EdgeLabel> olbl=g.getTopology().getEdgeLabel(schema, "E");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		assertFalse(olbl.get().getProperty("p1").isPresent());
		assertFalse(olbl.get().getProperty("p2").isPresent());
		
		assertEquals(0L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
			.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
			.out(SQLG_SCHEMA_OUT_EDGES_EDGE).has(SQLG_SCHEMA_EDGE_LABEL_NAME,"E")
			.out(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE).has(Topology.SQLG_SCHEMA_PROPERTY_NAME,"p1").count().next().longValue());
			
		assertEquals(0L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
				.out(SQLG_SCHEMA_OUT_EDGES_EDGE).has(SQLG_SCHEMA_EDGE_LABEL_NAME,"E")
				.out(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE).has(Topology.SQLG_SCHEMA_PROPERTY_NAME,"p2").count().next().longValue());
			
		assertFalse(columnExistsInSQL(schema, SchemaManager.EDGE_PREFIX+"E", "p1"));
		assertTrue(columnExistsInSQL(schema, SchemaManager.EDGE_PREFIX+"E", "p2"));
		
	}
	
	

	@Test
	public void testDeleteEdgePropertySchema() throws Exception {
		testDeleteEdgeProperty("MySchema");
	}
	
	@Test
	public void testDeleteEdgePropertyNoSchema() throws Exception {
		testDeleteEdgeProperty(this.sqlgGraph.getSqlDialect().getPublicSchema());
	}
	
	private void testDeleteEdgeProperty(String schema) throws Exception {
		try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
			String fullLabelA=schema.equals(this.sqlgGraph.getSqlDialect().getPublicSchema())?"A":schema+".A";
			String fullLabelB=schema.equals(this.sqlgGraph.getSqlDialect().getPublicSchema())?"B":schema+".B";
			Vertex a1 = this.sqlgGraph.addVertex(T.label, fullLabelA, "name", "A");
			Vertex b1 = this.sqlgGraph.addVertex(T.label, fullLabelB, "name", "B");
			Edge e1=a1.addEdge("E", b1, "p1","val1","p2","val2");
			
			assertTrue(e1.property("p1").isPresent());
			assertTrue(e1.property("p2").isPresent());
			checkEdgePropertyExistenceBeforeDeletion(schema);
			sqlgGraph.tx().commit();
			Object eid=e1.id();
			checkEdgePropertyExistenceBeforeDeletion(schema);
			e1=this.sqlgGraph.traversal().E(eid).next();
			assertTrue(e1.property("p1").isPresent());
			assertTrue(e1.property("p2").isPresent());
			
			TopologyListenerTest tlt=new TopologyListenerTest();
			this.sqlgGraph.getTopology().registerListener(tlt);
			
			EdgeLabel lbl=this.sqlgGraph.getTopology().getEdgeLabel(schema, "E").get();
			PropertyColumn p1=lbl.getProperty("p1").get();
			p1.remove(false);
			PropertyColumn p2=lbl.getProperty("p2").get();
			p2.remove(true);
			
			assertTrue(tlt.receivedEvent(p1, TopologyChangeAction.DELETE));
			assertTrue(tlt.receivedEvent(p2, TopologyChangeAction.DELETE));
			
			checkEdgePropertyExistenceAfterDeletion(this.sqlgGraph,schema);
			/* doesn't work because the topology allTableCache is only updated after commit
			a1=this.sqlgGraph.traversal().V(aid).next();
			assertFalse(a1.property("p1").isPresent());
			assertFalse(a1.property("p2").isPresent());
			*/
			this.sqlgGraph.tx().commit();
			checkEdgePropertyExistenceAfterDeletion(this.sqlgGraph,schema);
			
			
			e1=this.sqlgGraph.traversal().E(eid).next();
			assertFalse(e1.property("p1").isPresent());
			assertFalse(e1.property("p2").isPresent());
			
			Thread.sleep(1_000);
			checkEdgePropertyExistenceAfterDeletion(sqlgGraph1,schema);
		}
	}
}
