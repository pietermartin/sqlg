package org.umlg.sqlg.test.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.umlg.sqlg.structure.SchemaManager.EDGE_PREFIX;
import static org.umlg.sqlg.structure.SchemaManager.VERTEX_PREFIX;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_EDGE_INDEX_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_EDGE_LABEL_NAME;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_EDGE_PROPERTIES_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_INDEX_NAME;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_OUT_EDGES_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_PROPERTY_NAME;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_SCHEMA;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_VERTEX_INDEX_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.umlg.sqlg.structure.EdgeLabel;
import org.umlg.sqlg.structure.Index;
import org.umlg.sqlg.structure.IndexType;
import org.umlg.sqlg.structure.PropertyColumn;
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
	
	private boolean indexExistsInSQL(String schema,String table,String index) throws SQLException {
		try (ResultSet rs=this.sqlgGraph.tx().getConnection().getMetaData().getIndexInfo(null, schema, table, false, false)){
			while(rs.next()){
				String in=rs.getString("INDEX_NAME");
				if (index.equals(in)){
					return true;
				}
			}
			return false;
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
			.out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE).has(SQLG_SCHEMA_PROPERTY_NAME,"p1").count().next().longValue());
			
		assertEquals(1L,this.sqlgGraph.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
				.out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE).has(SQLG_SCHEMA_PROPERTY_NAME,"p2").count().next().longValue());
			
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
			.out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE).has(SQLG_SCHEMA_PROPERTY_NAME,"p1").count().next().longValue());
			
		assertEquals(0L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
				.out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE).has(SQLG_SCHEMA_PROPERTY_NAME,"p2").count().next().longValue());
			
		assertFalse(columnExistsInSQL(schema, VERTEX_PREFIX+"A", "p1"));
		assertTrue(columnExistsInSQL(schema, VERTEX_PREFIX+"A", "p2"));
		
	}
	
	
	@Test
	@Ignore
	public void testDeleteVertexPropertySchema() throws Exception {
		testDeleteVertexProperty("MySchema");
	}
	
	@Test
	@Ignore
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
			
		assertTrue(columnExistsInSQL(schema, EDGE_PREFIX+"E", "p1"));
		assertTrue(columnExistsInSQL(schema, EDGE_PREFIX+"E", "p2"));
		
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
			
		assertFalse(columnExistsInSQL(schema, EDGE_PREFIX+"E", "p1"));
		assertTrue(columnExistsInSQL(schema, EDGE_PREFIX+"E", "p2"));
		
	}
	
	

	@Test
	@Ignore
	public void testDeleteEdgePropertySchema() throws Exception {
		testDeleteEdgeProperty("MySchema");
	}
	
	@Test
	@Ignore
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
	
	private void checkIndexExistenceBeforeDeletion(String schema,Index i1,Index i2) throws Exception {
		Optional<VertexLabel> olbl=this.sqlgGraph.getTopology().getVertexLabel(schema, "A");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		assertTrue(olbl.get().getIndex(i1.getName()).isPresent());
		assertTrue(olbl.get().getIndex(i2.getName()).isPresent());
		
		assertEquals(1L,this.sqlgGraph.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
			.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
			.out(SQLG_SCHEMA_VERTEX_INDEX_EDGE).has(SQLG_SCHEMA_INDEX_NAME,i1.getName()).count().next().longValue());
			
		assertEquals(1L,this.sqlgGraph.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
				.out(SQLG_SCHEMA_VERTEX_INDEX_EDGE).has(SQLG_SCHEMA_INDEX_NAME,i2.getName()).count().next().longValue());
			
		assertTrue(indexExistsInSQL(schema, VERTEX_PREFIX+"A",i1.getName()));
		assertTrue(indexExistsInSQL(schema, VERTEX_PREFIX+"A", i2.getName()));
		
	}
	
	private void checkIndexExistenceAfterDeletion(SqlgGraph g,String schema,Index i1,Index i2) throws Exception {
		Optional<VertexLabel> olbl=g.getTopology().getVertexLabel(schema, "A");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		assertFalse(olbl.get().getProperty(i1.getName()).isPresent());
		assertFalse(olbl.get().getProperty(i2.getName()).isPresent());
		
		assertEquals(0L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
			.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
			.out(SQLG_SCHEMA_VERTEX_INDEX_EDGE).has(SQLG_SCHEMA_INDEX_NAME,i1.getName()).count().next().longValue());
			
		assertEquals(0L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
				.out(SQLG_SCHEMA_VERTEX_INDEX_EDGE).has(SQLG_SCHEMA_INDEX_NAME,i2.getName()).count().next().longValue());
			
		assertFalse(indexExistsInSQL(schema, VERTEX_PREFIX+"A", i1.getName()));
		assertTrue(indexExistsInSQL(schema, VERTEX_PREFIX+"A", i2.getName()));
		
	}
	
	@Test
	@Ignore
	public void testDeleteVertexIndexSchema() throws Exception {
		testDeleteVertexIndex("MySchema");
	}
	
	@Test
	@Ignore
	public void testDeleteVertexIndexNoSchema() throws Exception {
		testDeleteVertexIndex(this.sqlgGraph.getSqlDialect().getPublicSchema());
	}
	
	private void testDeleteVertexIndex(String schema) throws Exception {
		try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
			String fullLabel=schema.equals(this.sqlgGraph.getSqlDialect().getPublicSchema())?"A":schema+".A";
			this.sqlgGraph.addVertex(T.label, fullLabel, "name", "A","p1","val1","p2","val2");
			VertexLabel lbl=this.sqlgGraph.getTopology().getVertexLabel(schema, "A").get();
			
			Index i1=lbl.ensureIndexExists(IndexType.UNIQUE,Arrays.asList(lbl.getProperty("p1").get()) );
			Index i2=lbl.ensureIndexExists(IndexType.UNIQUE,Arrays.asList(lbl.getProperty("p2").get()) );
			
			
			checkIndexExistenceBeforeDeletion(schema,i1,i2);
			this.sqlgGraph.tx().commit();
			
			checkIndexExistenceBeforeDeletion(schema,i1,i2);
			
			TopologyListenerTest tlt=new TopologyListenerTest();
			this.sqlgGraph.getTopology().registerListener(tlt);
			
			i1.remove(false);
			i2.remove(true);
			
			assertTrue(tlt.receivedEvent(i1, TopologyChangeAction.DELETE));
			assertTrue(tlt.receivedEvent(i2, TopologyChangeAction.DELETE));
			
			checkIndexExistenceAfterDeletion(this.sqlgGraph,schema,i1,i2);
			this.sqlgGraph.tx().commit();
			checkIndexExistenceAfterDeletion(this.sqlgGraph,schema,i1,i2);
			
			
			Thread.sleep(1_000);
			checkIndexExistenceAfterDeletion(sqlgGraph1,schema,i1,i2);
		}
	}
	
	private void checkEdgeIndexExistenceBeforeDeletion(String schema,Index i1,Index i2) throws Exception {
		Optional<EdgeLabel> olbl=sqlgGraph.getTopology().getEdgeLabel(schema, "E");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		assertTrue(olbl.get().getIndex(i1.getName()).isPresent());
		assertTrue(olbl.get().getIndex(i2.getName()).isPresent());
		
		assertEquals(1L,this.sqlgGraph.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
			.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
			.out(SQLG_SCHEMA_OUT_EDGES_EDGE).has(SQLG_SCHEMA_EDGE_LABEL_NAME,"E")
			.out(SQLG_SCHEMA_EDGE_INDEX_EDGE).has(SQLG_SCHEMA_INDEX_NAME,i1.getName()).count().next().longValue());
			
		assertEquals(1L,this.sqlgGraph.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
				.out(SQLG_SCHEMA_OUT_EDGES_EDGE).has(SQLG_SCHEMA_EDGE_LABEL_NAME,"E")
				.out(SQLG_SCHEMA_EDGE_INDEX_EDGE).has(SQLG_SCHEMA_INDEX_NAME,i2.getName()).count().next().longValue());
			
		assertTrue(indexExistsInSQL(schema, EDGE_PREFIX+"E",i1.getName()));
		assertTrue(indexExistsInSQL(schema, EDGE_PREFIX+"E", i2.getName()));
		
	}
	
	private void checkEdgeIndexExistenceAfterDeletion(SqlgGraph g,String schema,Index i1,Index i2) throws Exception {
		Optional<EdgeLabel> olbl=g.getTopology().getEdgeLabel(schema, "E");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		assertFalse(olbl.get().getProperty(i1.getName()).isPresent());
		assertFalse(olbl.get().getProperty(i2.getName()).isPresent());
		
		assertEquals(0L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
			.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
			.out(SQLG_SCHEMA_OUT_EDGES_EDGE).has(SQLG_SCHEMA_EDGE_LABEL_NAME,"E")
			.out(SQLG_SCHEMA_EDGE_INDEX_EDGE).has(SQLG_SCHEMA_INDEX_NAME,i1.getName()).count().next().longValue());
			
		assertEquals(0L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
				.out(SQLG_SCHEMA_OUT_EDGES_EDGE).has(SQLG_SCHEMA_EDGE_LABEL_NAME,"E")
				.out(SQLG_SCHEMA_EDGE_INDEX_EDGE).has(SQLG_SCHEMA_INDEX_NAME,i2.getName()).count().next().longValue());
			
		assertFalse(indexExistsInSQL(schema, EDGE_PREFIX+"E", i1.getName()));
		assertTrue(indexExistsInSQL(schema, EDGE_PREFIX+"E", i2.getName()));
		
	}
	
	@Test
	public void testDeleteEdgeIndexSchema() throws Exception {
		testDeleteEdgeIndex("MySchema");
	}
	
	@Test
	public void testDeleteEdgeIndexNoSchema() throws Exception {
		testDeleteEdgeIndex(this.sqlgGraph.getSqlDialect().getPublicSchema());
	}
	
	private void testDeleteEdgeIndex(String schema) throws Exception {
		try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
			String fullLabelA=schema.equals(this.sqlgGraph.getSqlDialect().getPublicSchema())?"A":schema+".A";
			String fullLabelB=schema.equals(this.sqlgGraph.getSqlDialect().getPublicSchema())?"B":schema+".B";
			Vertex a1 = this.sqlgGraph.addVertex(T.label, fullLabelA, "name", "A");
			Vertex b1 = this.sqlgGraph.addVertex(T.label, fullLabelB, "name", "B");
			a1.addEdge("E", b1, "p1","val1","p2","val2");
			
			EdgeLabel lbl=this.sqlgGraph.getTopology().getEdgeLabel(schema, "E").get();
			Index i1=lbl.ensureIndexExists(IndexType.UNIQUE,Arrays.asList(lbl.getProperty("p1").get()) );
			Index i2=lbl.ensureIndexExists(IndexType.UNIQUE,Arrays.asList(lbl.getProperty("p2").get()) );
			
			
			checkEdgeIndexExistenceBeforeDeletion(schema,i1,i2);
			this.sqlgGraph.tx().commit();
			
			checkEdgeIndexExistenceBeforeDeletion(schema,i1,i2);
			
			TopologyListenerTest tlt=new TopologyListenerTest();
			this.sqlgGraph.getTopology().registerListener(tlt);
			
			i1.remove(false);
			i2.remove(true);
			
			assertTrue(tlt.receivedEvent(i1, TopologyChangeAction.DELETE));
			assertTrue(tlt.receivedEvent(i2, TopologyChangeAction.DELETE));
			
			checkEdgeIndexExistenceAfterDeletion(this.sqlgGraph,schema,i1,i2);
			this.sqlgGraph.tx().commit();
			checkEdgeIndexExistenceAfterDeletion(this.sqlgGraph,schema,i1,i2);
			
			
			Thread.sleep(1_000);
			checkEdgeIndexExistenceAfterDeletion(sqlgGraph1,schema,i1,i2);
		}
	}
}
