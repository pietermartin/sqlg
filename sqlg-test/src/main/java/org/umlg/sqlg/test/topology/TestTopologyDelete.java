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
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_INDEX_NAME;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_IN_EDGES_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_OUT_EDGES_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_PROPERTY_NAME;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_SCHEMA;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_VERTEX_INDEX_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_VERTEX_LABEL;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.umlg.sqlg.structure.EdgeLabel;
import org.umlg.sqlg.structure.EdgeRole;
import org.umlg.sqlg.structure.GlobalUniqueIndex;
import org.umlg.sqlg.structure.Index;
import org.umlg.sqlg.structure.IndexType;
import org.umlg.sqlg.structure.PropertyColumn;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.Schema;
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
@RunWith(Parameterized.class)
public class TestTopologyDelete extends BaseTest {
	
	@Parameters(name = "{index}: schema:{0}, preserve:{1}, rollback:{2}")
    public static Collection<Object[]> data() {
    	List<Object[]> l=new ArrayList<>();
    	String[] schemas=new String[]{null,"MySchema"};
    	boolean[] preserve=new boolean[]{true,false};
    	boolean[] rollback=new boolean[]{true,false};
    	
    	for (String s:schemas){
    		for (boolean p:preserve){
    			for (boolean r:rollback){
    				l.add(new Object[]{s,p,r});
    			}
    		}
    	}
        return l;
    }
    
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
	
	@Parameter(0)
	public String schema;
	@Parameter(1)
	public boolean preserve;
	@Parameter(2)
	public boolean rollback;
	
	@Before
	public void buildLoops(){
		if (schema==null){
			schema=this.sqlgGraph.getSqlDialect().getPublicSchema();
		}
	}
	
	private boolean schemaExistsInSQL(String schema) throws SQLException {
		try (ResultSet rs=this.sqlgGraph.tx().getConnection().getMetaData().getSchemas(null, schema)){
			return rs.next();
		}
		
	}
	
	
	private boolean tableExistsInSQL(String schema,String table) throws SQLException {
		try (ResultSet rs=this.sqlgGraph.tx().getConnection().getMetaData().getTables(null, schema, table, null)){
			return rs.next();
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
	public void testDeleteVertexProperty() throws Exception {
		try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
			String fullLabel=getLabel(schema,"A");
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
			if (rollback){
				this.sqlgGraph.tx().rollback();
				checkPropertyExistenceBeforeDeletion(schema);
			} else {
				TopologyListenerTest tlt1=new TopologyListenerTest();
				sqlgGraph1.getTopology().registerListener(tlt1);
				this.sqlgGraph.tx().commit();
				checkPropertyExistenceAfterDeletion(this.sqlgGraph,schema);
				
				a1=this.sqlgGraph.traversal().V(aid).next();
				assertFalse(a1.property("p1").isPresent());
				assertFalse(a1.property("p2").isPresent());
			
				Thread.sleep(1_000);
				checkPropertyExistenceAfterDeletion(sqlgGraph1,schema);
				assertTrue(tlt1.receivedEvent(p1, TopologyChangeAction.DELETE));
				assertTrue(tlt1.receivedEvent(p2, TopologyChangeAction.DELETE));
			}
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
	public void testDeleteEdgeProperty() throws Exception {
		try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
			String fullLabelA=getLabel(schema,"A");
			String fullLabelB=getLabel(schema,"B");
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
			if (rollback){
				this.sqlgGraph.tx().rollback();
				checkEdgePropertyExistenceBeforeDeletion(schema);
			} else {
				TopologyListenerTest tlt1=new TopologyListenerTest();
				sqlgGraph1.getTopology().registerListener(tlt1);
				this.sqlgGraph.tx().commit();
				checkEdgePropertyExistenceAfterDeletion(this.sqlgGraph,schema);
				
				
				e1=this.sqlgGraph.traversal().E(eid).next();
				assertFalse(e1.property("p1").isPresent());
				assertFalse(e1.property("p2").isPresent());
				
				Thread.sleep(1_000);
				checkEdgePropertyExistenceAfterDeletion(sqlgGraph1,schema);
				assertTrue(tlt1.receivedEvent(p1, TopologyChangeAction.DELETE));
				assertTrue(tlt1.receivedEvent(p2, TopologyChangeAction.DELETE));
			}
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
	public void testDeleteVertexIndex() throws Exception {
		try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
			String fullLabel=getLabel(schema,"A");
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
			
			if (rollback){
				this.sqlgGraph.tx().rollback();
				checkIndexExistenceBeforeDeletion(schema,i1,i2);
			} else {
				TopologyListenerTest tlt1=new TopologyListenerTest();
				sqlgGraph1.getTopology().registerListener(tlt1);
				
				
				this.sqlgGraph.tx().commit();
				checkIndexExistenceAfterDeletion(this.sqlgGraph,schema,i1,i2);
				
				
				Thread.sleep(1_000);
				checkIndexExistenceAfterDeletion(sqlgGraph1,schema,i1,i2);
				assertTrue(tlt1.receivedEvent(i1, TopologyChangeAction.DELETE));
				assertTrue(tlt1.receivedEvent(i2, TopologyChangeAction.DELETE));
			}
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
	public void testDeleteEdgeIndex() throws Exception {
		try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
			String fullLabelA=getLabel(schema,"A");
			String fullLabelB=getLabel(schema,"B");
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
			if (rollback){
				this.sqlgGraph.tx().rollback();
				checkEdgeIndexExistenceBeforeDeletion(schema,i1,i2);
			} else {
			
				TopologyListenerTest tlt1=new TopologyListenerTest();
				sqlgGraph1.getTopology().registerListener(tlt1);
				
				this.sqlgGraph.tx().commit();
				checkEdgeIndexExistenceAfterDeletion(this.sqlgGraph,schema,i1,i2);
				
				
				Thread.sleep(1_000);
				checkEdgeIndexExistenceAfterDeletion(sqlgGraph1,schema,i1,i2);
				assertTrue(tlt1.receivedEvent(i1, TopologyChangeAction.DELETE));
				assertTrue(tlt1.receivedEvent(i2, TopologyChangeAction.DELETE));
			}
		}
	}
	
	private void checkEdgeExistenceBeforeDeletion(SqlgGraph g,String schema) throws Exception {
		Optional<EdgeLabel> olbl=g.getTopology().getEdgeLabel(schema, "E1");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		olbl=g.getTopology().getEdgeLabel(schema, "E2");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		
		VertexLabel a=g.getTopology().getVertexLabel(schema, "A").get();
		assertTrue(a.getOutEdgeLabel("E1").isPresent());
		assertTrue(a.getOutEdgeLabel("E2").isPresent());
	
		
		assertEquals(1L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
			.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
			.out(SQLG_SCHEMA_OUT_EDGES_EDGE).has(SQLG_SCHEMA_EDGE_LABEL_NAME,"E1").count().next().longValue());
			
		assertEquals(1L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
				.out(SQLG_SCHEMA_OUT_EDGES_EDGE).has(SQLG_SCHEMA_EDGE_LABEL_NAME,"E2").count().next().longValue());
			
		assertTrue(tableExistsInSQL(schema, EDGE_PREFIX+"E1"));
		assertTrue(tableExistsInSQL(schema, EDGE_PREFIX+"E2"));
		
	}
	
	private void checkEdgeExistenceAfterDeletion(SqlgGraph g,String schema) throws Exception {
		Optional<EdgeLabel> olbl=g.getTopology().getEdgeLabel(schema, "E1");
		assertNotNull(olbl);
		assertFalse(olbl.isPresent());
		olbl=g.getTopology().getEdgeLabel(schema, "E2");
		assertNotNull(olbl);
		assertFalse(olbl.isPresent());
		
		VertexLabel a=g.getTopology().getVertexLabel(schema, "A").get();
		assertFalse(a.getOutEdgeLabel("E1").isPresent());
		assertFalse(a.getOutEdgeLabel("E2").isPresent());
		
		assertEquals(0L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
				.out(SQLG_SCHEMA_OUT_EDGES_EDGE).has(SQLG_SCHEMA_EDGE_LABEL_NAME,"E1").count().next().longValue());
				
		assertEquals(0L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
				.out(SQLG_SCHEMA_OUT_EDGES_EDGE).has(SQLG_SCHEMA_EDGE_LABEL_NAME,"E2").count().next().longValue());
			
		assertFalse(tableExistsInSQL(schema, EDGE_PREFIX+"E1"));
		assertTrue(tableExistsInSQL(schema, EDGE_PREFIX+"E2"));
		
	}
	
	private String getLabel(String schema, String type){
		return schema.equals(this.sqlgGraph.getSqlDialect().getPublicSchema())?type:schema+"."+type;
		
	}
	
	@Test
	public void testDeleteEdgeLabel() throws Exception {
		try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
			String fullLabelA=getLabel(schema,"A");
			String fullLabelB=getLabel(schema,"B");
			Vertex a1 = this.sqlgGraph.addVertex(T.label, fullLabelA, "name", "A");
			Vertex b1 = this.sqlgGraph.addVertex(T.label, fullLabelB, "name", "B1");
			Vertex b2 = this.sqlgGraph.addVertex(T.label, fullLabelB, "name", "B2");
			a1.addEdge("E1", b1);
			a1.addEdge("E2", b2);
			
						
			checkEdgeExistenceBeforeDeletion(this.sqlgGraph,schema);
			this.sqlgGraph.tx().commit();
			
			checkEdgeExistenceBeforeDeletion(this.sqlgGraph,schema);
			Thread.sleep(1_000);
			checkEdgeExistenceBeforeDeletion(this.sqlgGraph1,schema);
			
			TopologyListenerTest tlt=new TopologyListenerTest();
			this.sqlgGraph.getTopology().registerListener(tlt);
			
			EdgeLabel e1=this.sqlgGraph.getTopology().getEdgeLabel(schema, "E1").get();
			EdgeLabel e2=this.sqlgGraph.getTopology().getEdgeLabel(schema, "E2").get();
			
			e1.remove(false);
			e2.remove(true);
			
			assertTrue(tlt.receivedEvent(e1, TopologyChangeAction.DELETE));
			assertTrue(tlt.receivedEvent(e2, TopologyChangeAction.DELETE));
			
			checkEdgeExistenceAfterDeletion(this.sqlgGraph,schema);
			
			if (rollback){
				this.sqlgGraph.tx().rollback();
				checkEdgeExistenceBeforeDeletion(this.sqlgGraph,schema);
			} else {
				
				TopologyListenerTest tlt1=new TopologyListenerTest();
				sqlgGraph1.getTopology().registerListener(tlt1);
				this.sqlgGraph.tx().commit();
				checkEdgeExistenceAfterDeletion(this.sqlgGraph,schema);
				
				
				Thread.sleep(1_000);
				checkEdgeExistenceAfterDeletion(sqlgGraph1,schema);
				assertTrue(tlt1.receivedEvent(e1, TopologyChangeAction.DELETE));
				assertTrue(tlt1.receivedEvent(e2, TopologyChangeAction.DELETE));
			}
		}
	}
	
	private void checkEdgeRoleExistenceBeforeDeletion(String schemaOut,String schemaIn) throws Exception {
		Optional<EdgeLabel> olbl=this.sqlgGraph.getTopology().getEdgeLabel(schemaOut, "E1");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		
		VertexLabel a=this.sqlgGraph.getTopology().getVertexLabel(schemaOut, "A").get();
		VertexLabel b=this.sqlgGraph.getTopology().getVertexLabel(schemaOut, "B").get();
		VertexLabel pa=this.sqlgGraph.getTopology().getVertexLabel(schemaIn, "A").get();
		VertexLabel pb=this.sqlgGraph.getTopology().getVertexLabel(schemaIn, "B").get();
		assertEquals(1,a.getOutEdgeLabels().size());
		assertEquals(1,b.getOutEdgeLabels().size());
		assertEquals(1,pb.getInEdgeLabels().size());
		assertEquals(1,pa.getInEdgeLabels().size());
		
		Set<VertexLabel> inLbls=olbl.get().getInVertexLabels();
		assertEquals(2,inLbls.size());
		assertTrue(inLbls.contains(pa));
		assertTrue(inLbls.contains(pb));
		
		Set<EdgeRole> inRoles=olbl.get().getInEdgeRoles();
		assertEquals(2,inRoles.size());
		
		
		Set<VertexLabel> outLbls=olbl.get().getOutVertexLabels();
		assertEquals(2,outLbls.size());
		assertTrue(outLbls.contains(a));
		assertTrue(outLbls.contains(b));
		
		Set<EdgeRole> outRoles=olbl.get().getOutEdgeRoles();
		assertEquals(2,outRoles.size());
		
		
		assertTrue(tableExistsInSQL(schemaOut, EDGE_PREFIX+"E1"));
		assertTrue(columnExistsInSQL(schemaOut, EDGE_PREFIX+"E1", schemaOut+".A"+SchemaManager.OUT_VERTEX_COLUMN_END));
		assertTrue(columnExistsInSQL(schemaOut, EDGE_PREFIX+"E1", schemaOut+".B"+SchemaManager.OUT_VERTEX_COLUMN_END));
		assertTrue(columnExistsInSQL(schemaOut, EDGE_PREFIX+"E1", schemaIn+".A"+SchemaManager.IN_VERTEX_COLUMN_END));
		assertTrue(columnExistsInSQL(schemaOut, EDGE_PREFIX+"E1", schemaIn+".B"+SchemaManager.IN_VERTEX_COLUMN_END));
	}
	
	private void checkEdgeRoleExistenceAfterRoleDeletion(SqlgGraph g,String schemaOut,String schemaIn) throws Exception {
		Optional<EdgeLabel> olbl=g.getTopology().getEdgeLabel(schemaOut, "E1");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		
		VertexLabel a=g.getTopology().getVertexLabel(schemaOut, "A").get();
		VertexLabel b=g.getTopology().getVertexLabel(schemaOut, "B").get();
		VertexLabel pa=g.getTopology().getVertexLabel(schemaIn, "A").get();
		VertexLabel pb=g.getTopology().getVertexLabel(schemaIn, "B").get();
		assertEquals(1,a.getOutEdgeLabels().size());
		assertEquals(0,b.getOutEdgeLabels().size());
		assertEquals(1,pb.getInEdgeLabels().size());
		assertEquals(0,pa.getInEdgeLabels().size());
		
		Set<VertexLabel> inLbls=olbl.get().getInVertexLabels();
		assertEquals(1,inLbls.size());
		assertFalse(inLbls.contains(pa));
		assertTrue(inLbls.contains(pb));
		
		Set<EdgeRole> inRoles=olbl.get().getInEdgeRoles();
		assertEquals(1,inRoles.size());
		assertEquals("B",inRoles.iterator().next().getVertexLabel().getName());
		
		
		Set<VertexLabel> outLbls=olbl.get().getOutVertexLabels();
		assertEquals(1,outLbls.size());
		assertTrue(outLbls.contains(a));
		assertFalse(outLbls.contains(b));
		
		Set<EdgeRole> outRoles=olbl.get().getOutEdgeRoles();
		assertEquals(1,outRoles.size());
		assertEquals("A",outRoles.iterator().next().getVertexLabel().getName());
		
		assertTrue(tableExistsInSQL(schemaOut, EDGE_PREFIX+"E1"));
		assertTrue(columnExistsInSQL(schemaOut, EDGE_PREFIX+"E1", schemaOut+".A"+SchemaManager.OUT_VERTEX_COLUMN_END));
		assertFalse(columnExistsInSQL(schemaOut, EDGE_PREFIX+"E1", schemaOut+".B"+SchemaManager.OUT_VERTEX_COLUMN_END));
		assertTrue(columnExistsInSQL(schemaOut, EDGE_PREFIX+"E1", schemaIn+".A"+SchemaManager.IN_VERTEX_COLUMN_END));
		assertTrue(columnExistsInSQL(schemaOut, EDGE_PREFIX+"E1", schemaIn+".B"+SchemaManager.IN_VERTEX_COLUMN_END));
	}
	
	@Test
	public void testDeleteEdgeRoleOut() throws Exception{
		testDeleteEdgeRole(schema,"MySchema2");
	}
	
	@Test
	public void testDeleteEdgeRoleIn() throws Exception{
		testDeleteEdgeRole("MySchema2",schema);
	}
	
	private void testDeleteEdgeRole(String schemaOut,String schemaIn) throws Exception {
		try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
			String Aout=getLabel(schemaOut,"A");
			String Bout=getLabel(schemaOut,"B");
			String Ain=getLabel(schemaIn,"A");
			String Bin=getLabel(schemaIn,"B");
			Vertex a1 = this.sqlgGraph.addVertex(T.label, Aout, "name", "A1");
			Vertex a2 = this.sqlgGraph.addVertex(T.label, Ain, "name", "A2");
			
			Vertex b1 = this.sqlgGraph.addVertex(T.label, Bout, "name", "B1");
			Vertex b2 = this.sqlgGraph.addVertex(T.label, Bin, "name", "B2");
			a1.addEdge("E1", b2);
			b1.addEdge("E1", a2);
			
			//checkEdgeRoleExistenceBeforeDeletion();
			this.sqlgGraph.tx().commit();
			
			checkEdgeRoleExistenceBeforeDeletion(schemaOut,schemaIn);
			
			TopologyListenerTest tlt=new TopologyListenerTest();
			this.sqlgGraph.getTopology().registerListener(tlt);
			
			
			EdgeLabel lbl=this.sqlgGraph.getTopology().getEdgeLabel(schemaOut, "E1").get();
			for (EdgeRole er:lbl.getOutEdgeRoles()){
				if (er.getVertexLabel().getName().equals("B")){
					er.remove(false);
					assertTrue(tlt.receivedEvent(er, TopologyChangeAction.DELETE));
					
				}
			}
			for (EdgeRole er:lbl.getInEdgeRoles()){
				if (er.getVertexLabel().getName().equals("A")){
					er.remove(true);
					assertTrue(tlt.receivedEvent(er, TopologyChangeAction.DELETE));
					
				}
			}
			if (rollback){
				this.sqlgGraph.tx().rollback();
				checkEdgeRoleExistenceBeforeDeletion(schemaOut,schemaIn);
				
			} else {
				this.sqlgGraph.tx().commit();
				checkEdgeRoleExistenceAfterRoleDeletion(this.sqlgGraph,schemaOut,schemaIn);
				Thread.sleep(1_000);
				checkEdgeRoleExistenceAfterRoleDeletion(sqlgGraph1,schemaOut,schemaIn);
				
				assertFalse(b1.edges(Direction.OUT, "E1").hasNext());
				assertTrue(a1.edges(Direction.OUT, "E1").hasNext());
				
				lbl.getOutEdgeRoles().iterator().next().remove(false);
				assertFalse(this.sqlgGraph.getTopology().getEdgeLabel(schemaOut, "E1").isPresent());
				this.sqlgGraph.tx().commit();
				assertFalse(this.sqlgGraph.getTopology().getEdgeLabel(schemaOut, "E1").isPresent());			
			}
		}
	}
	
	private void checkVertexLabelBeforeDeletion(SqlgGraph g,String schema) throws Exception {
		Optional<VertexLabel> olbl=g.getTopology().getVertexLabel(schema, "A");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		
		olbl=g.getTopology().getVertexLabel(schema, "B");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		
		olbl=g.getTopology().getVertexLabel(schema, "C");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		
		olbl=g.getTopology().getVertexLabel(schema, "D");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		
		assertEquals(1L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A").count().next().longValue());
	
		assertEquals(1L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"B").count().next().longValue());
	
		assertEquals(1L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"C").count().next().longValue());
	
		assertEquals(1L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"D").count().next().longValue());
	
		
		assertTrue(tableExistsInSQL(schema, VERTEX_PREFIX+"A"));
		assertTrue(tableExistsInSQL(schema, VERTEX_PREFIX+"B"));
		assertTrue(tableExistsInSQL(schema, VERTEX_PREFIX+"C"));
		assertTrue(tableExistsInSQL(schema, VERTEX_PREFIX+"D"));
		
		
		Optional<EdgeLabel> oelbl=g.getTopology().getEdgeLabel(schema, "E");
		assertNotNull(oelbl);
		assertTrue(oelbl.isPresent());
		EdgeLabel elbl=oelbl.get();
		assertEquals(3,elbl.getInVertexLabels().size());
		assertEquals(3,elbl.getOutVertexLabels().size());
		
	}
	
	private void checkVertexLabelAfterDeletion(SqlgGraph g,String schema) throws Exception {
		Optional<VertexLabel> olbl=g.getTopology().getVertexLabel(schema, "A");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		
		olbl=g.getTopology().getVertexLabel(schema, "B");
		assertNotNull(olbl);
		assertTrue(olbl.isPresent());
		
		olbl=g.getTopology().getVertexLabel(schema, "C");
		assertNotNull(olbl);
		assertFalse(olbl.isPresent());
		
		olbl=g.getTopology().getVertexLabel(schema, "D");
		assertNotNull(olbl);
		assertFalse(olbl.isPresent());
		
		assertEquals(1L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A").count().next().longValue());
	
		assertEquals(1L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"B").count().next().longValue());
	
		assertEquals(0L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"C").count().next().longValue());
	
		assertEquals(0L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"D").count().next().longValue());
	
		
		assertTrue(tableExistsInSQL(schema, VERTEX_PREFIX+"A"));
		assertTrue(tableExistsInSQL(schema, VERTEX_PREFIX+"B"));
		assertFalse(tableExistsInSQL(schema, VERTEX_PREFIX+"C"));
		assertTrue(tableExistsInSQL(schema, VERTEX_PREFIX+"D"));
		
		
		Optional<EdgeLabel> oelbl=g.getTopology().getEdgeLabel(schema, "E");
		assertNotNull(oelbl);
		assertTrue(oelbl.isPresent());
		EdgeLabel elbl=oelbl.get();
		assertEquals(1,elbl.getInVertexLabels().size());
		assertEquals(2,elbl.getOutVertexLabels().size());
	}
	
	@Test
	public void testDeleteVertexLabel() throws Exception {
		try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
			String A=getLabel(schema,"A");
			String B=getLabel(schema,"B");
			String C=getLabel(schema,"C");
			String D=getLabel(schema,"D");
			Vertex a = this.sqlgGraph.addVertex(T.label, A, "name", "A");
			Vertex b = this.sqlgGraph.addVertex(T.label, B, "name", "B");
			Vertex c = this.sqlgGraph.addVertex(T.label, C, "name", "C", "p1","v1");
			Vertex d = this.sqlgGraph.addVertex(T.label, D, "name", "D", "p1","v1");
			a.addEdge("E", b);
			b.addEdge("E", c);
			c.addEdge("E", d);
			
			//checkVertexLabelBeforeDeletion(sqlgGraph,schema);
			this.sqlgGraph.tx().commit();
			checkVertexLabelBeforeDeletion(sqlgGraph,schema);
			Thread.sleep(1_000);
			checkVertexLabelBeforeDeletion(sqlgGraph1,schema);
			
			
			TopologyListenerTest tlt=new TopologyListenerTest();
			this.sqlgGraph.getTopology().registerListener(tlt);
			
			VertexLabel lblc=this.sqlgGraph.getTopology().getVertexLabel(schema, "C").get();
			lblc.remove(false);
			assertTrue(tlt.receivedEvent(lblc, TopologyChangeAction.DELETE));
			
			VertexLabel lbld=this.sqlgGraph.getTopology().getVertexLabel(schema, "D").get();
			lbld.remove(true);
			assertTrue(tlt.receivedEvent(lbld, TopologyChangeAction.DELETE));
			
			if (rollback){
				sqlgGraph.tx().rollback();
				checkVertexLabelBeforeDeletion(sqlgGraph,schema);
			} else {
				//checkVertexLabelAfterDeletion(sqlgGraph,schema);
				sqlgGraph.tx().commit();
				checkVertexLabelAfterDeletion(sqlgGraph,schema);
				Thread.sleep(1_000);
				checkVertexLabelAfterDeletion(sqlgGraph1,schema);
				
				
				VertexLabel lblb=this.sqlgGraph.getTopology().getVertexLabel(schema, "B").get();
				lblb.remove(true);
				assertTrue(tlt.receivedEvent(lblb, TopologyChangeAction.DELETE));
				
				assertFalse(sqlgGraph.getTopology().getEdgeLabel(schema, "E").isPresent());
				
				TopologyListenerTest tlt1=new TopologyListenerTest();
				sqlgGraph1.getTopology().registerListener(tlt1);
					
				sqlgGraph.tx().commit();
				assertFalse(sqlgGraph.getTopology().getEdgeLabel(schema, "E").isPresent());
				Thread.sleep(1_000);
				assertFalse(sqlgGraph1.getTopology().getEdgeLabel(schema, "E").isPresent());
				assertTrue(tlt1.receivedEvent(lblb, TopologyChangeAction.DELETE));
			}
		}
	}
	
	private void checkGlobalUniqueIndexBeforeDeletion(SqlgGraph g,String schema,GlobalUniqueIndex gui1,GlobalUniqueIndex gui2) throws Exception {
		Set<GlobalUniqueIndex> guis=g.getTopology().getGlobalUniqueIndexes();
		assertNotNull(guis);
		assertEquals(2,guis.size());
		assertTrue(guis.contains(gui1));
		assertTrue(guis.contains(gui2));
		
		Map<String,GlobalUniqueIndex> mguis=g.getTopology().getGlobalUniqueIndexSchema().getGlobalUniqueIndexes();
		assertNotNull(mguis);
		assertEquals(2,mguis.size());
		assertTrue(mguis.containsKey(gui1.getName()));
		assertTrue(mguis.containsKey(gui2.getName()));
		String name1=gui1.getName();
		assertEquals(1,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
                    "name",name1 ).count().next().longValue());
                    
		assertEquals(2,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
                "name",name1 ).out(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE).count().next().longValue());
		assertEquals(1,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
                "name",name1 ).out(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE)
				.has("name","name1").count().next().longValue());
    
		assertTrue(tableExistsInSQL(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA, VERTEX_PREFIX+name1));
	
		String name2=gui2.getName();
		assertEquals(1,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
                    "name",name2 ).count().next().longValue());
                    
		assertEquals(2,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
                "name",name2 ).out(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE).count().next().longValue());
		assertEquals(1,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
                "name",name2 ).out(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE)
				.has("name","name1").count().next().longValue());
    
		assertTrue(tableExistsInSQL(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA, VERTEX_PREFIX+name2));
	}
	
	private void checkGlobalUniqueIndexAfterDeletion(SqlgGraph g,String schema,GlobalUniqueIndex gui1,GlobalUniqueIndex gui2) throws Exception {
		String name1=gui1.getName();
		String name2=gui2.getName();
		
		Set<GlobalUniqueIndex> guis=g.getTopology().getGlobalUniqueIndexes();
		assertNotNull(guis);
		assertEquals(0,guis.size());
		Map<String,GlobalUniqueIndex> mguis=g.getTopology().getGlobalUniqueIndexSchema().getGlobalUniqueIndexes();
		assertNotNull(mguis);
		assertEquals(0,mguis.size());
		assertEquals(0,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
                    "name",name1 ).count().next().longValue());
                    
		assertEquals(0,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
                "name",name1 ).out(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE).count().next().longValue());
		assertEquals(0,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
                "name",name1 ).out(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE)
				.has("name","name1").count().next().longValue());
    
		assertFalse(tableExistsInSQL(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA, VERTEX_PREFIX+name1));
	
		assertEquals(0,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
                    "name",name2 ).count().next().longValue());
                    
		assertEquals(0,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
                "name",name2 ).out(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE).count().next().longValue());
		assertEquals(0,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
                "name",name2 ).out(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE)
				.has("name","name1").count().next().longValue());
    
		assertTrue(tableExistsInSQL(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA, VERTEX_PREFIX+name2));
	}
	
	@Test
	public void testDeleteGlobalUniqueIndex() throws Exception {
		try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
		    Map<String, PropertyType> properties = new HashMap<>();
	        properties.put("name1", PropertyType.STRING);
	        properties.put("name2", PropertyType.STRING);
	        
	        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", properties);
	        assertTrue(aVertexLabel.isUncommitted());
	        Set<PropertyColumn> globalUniqueIndexPropertyColumns = new HashSet<>();
	        globalUniqueIndexPropertyColumns.addAll(new HashSet<>(aVertexLabel.getProperties().values()));
	        GlobalUniqueIndex gui1=this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(globalUniqueIndexPropertyColumns);
	        
	        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B", properties);
	        assertTrue(bVertexLabel.isUncommitted());
	        globalUniqueIndexPropertyColumns = new HashSet<>();
	        globalUniqueIndexPropertyColumns.addAll(new HashSet<>(bVertexLabel.getProperties().values()));
	        GlobalUniqueIndex gui2=this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(globalUniqueIndexPropertyColumns);
	        
	        checkGlobalUniqueIndexBeforeDeletion(this.sqlgGraph,schema,gui1,gui2);
	        this.sqlgGraph.tx().commit();
	        checkGlobalUniqueIndexBeforeDeletion(this.sqlgGraph,schema,gui1,gui2);
	        Thread.sleep(1_000);
	        checkGlobalUniqueIndexBeforeDeletion(sqlgGraph1,schema,gui1,gui2);
	        
	        TopologyListenerTest tlt=new TopologyListenerTest();
			this.sqlgGraph.getTopology().registerListener(tlt);
	        
	        gui1.remove(false);
	        assertTrue(tlt.receivedEvent(gui1, TopologyChangeAction.DELETE));
	        
	        gui2.remove(true);
	        assertTrue(tlt.receivedEvent(gui2, TopologyChangeAction.DELETE));
	        
	        checkGlobalUniqueIndexAfterDeletion(this.sqlgGraph,schema,gui1,gui2);
	        
	        if (rollback){
	        	this.sqlgGraph.tx().rollback();
	        	checkGlobalUniqueIndexBeforeDeletion(this.sqlgGraph,schema,gui1,gui2);
		        
	        } else {
	        
		        TopologyListenerTest tlt1=new TopologyListenerTest();
				sqlgGraph1.getTopology().registerListener(tlt1);
		        
		        this.sqlgGraph.tx().commit();
		        checkGlobalUniqueIndexAfterDeletion(this.sqlgGraph,schema,gui1,gui2);
		        Thread.sleep(1_000);
		        checkGlobalUniqueIndexAfterDeletion(sqlgGraph1,schema,gui1,gui2);
		        assertTrue(tlt1.receivedEvent(gui1, TopologyChangeAction.DELETE));
		        assertTrue(tlt1.receivedEvent(gui2, TopologyChangeAction.DELETE));
	        }
		}
	}
	
	private void testSchemaBeforeDeletion(SqlgGraph g,String schema,GlobalUniqueIndex gui) throws Exception {
		Optional<Schema> osch=g.getTopology().getSchema(schema);
		assertNotNull(osch);
		assertTrue(osch.isPresent());
		Schema sch=osch.get();
		assertTrue(g.getTopology().getSchemas().contains(sch));
		
		assertEquals(1,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA,
                    "name", schema).count().next().longValue());
		assertEquals(2,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA,
                "name", schema).out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).count().next().longValue());
		assertEquals(1,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA,
                "name", schema).out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has("name","A")
				.out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE)
				.count().next().longValue());
		assertEquals(1,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA,
                "name", schema).out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has("name","B")
				.out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE)
				.count().next().longValue());	
		assertEquals(1,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA,
                "name", schema).out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has("name","A")
				.out(SQLG_SCHEMA_VERTEX_INDEX_EDGE)
				.count().next().longValue());
		assertEquals(1,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA,
                "name", schema).out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has("name","A")
				.out(SQLG_SCHEMA_OUT_EDGES_EDGE).has("name","E")
				.count().next().longValue());
		assertEquals(1,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA,
                "name", schema).out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has("name","A")
				.out(SQLG_SCHEMA_OUT_EDGES_EDGE).has("name","E")
				.out(SQLG_SCHEMA_EDGE_INDEX_EDGE)
				.count().next().longValue());
		
		assertEquals(1,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
                "name",gui.getName() ).count().next().longValue());
                
		assertTrue(schemaExistsInSQL(schema));
	}
	
	private void testSchemaAfterDeletion(SqlgGraph g,String schema,GlobalUniqueIndex gui,boolean preserveData) throws Exception {
		Optional<Schema> osch=g.getTopology().getSchema(schema);
		assertNotNull(osch);
		assertFalse(osch.isPresent());
		
		for (Schema s:g.getTopology().getSchemas()){
			assertFalse(s.getName().equals(schema));
		}
		
		assertEquals(0,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA,
                    "name", schema).count().next().longValue());
		assertEquals(0,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_VERTEX_LABEL,
                "name","A")
				.out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE)
				.count().next().longValue());
		assertEquals(0,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_VERTEX_LABEL,"name","B")
				.out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE)
				.count().next().longValue());	
		assertEquals(0,g.topology().V().has(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_EDGE_LABEL,
                "name", "E")
				.count().next().longValue());
		
		assertEquals(0,g.topology().V().has(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
                "name",gui.getName() ).count().next().longValue());
               
		if (preserveData){
			assertTrue(schemaExistsInSQL(schema));
		} else {
			assertFalse(schemaExistsInSQL(schema));
		}
	}
	
	
	
	@Test
	public void testDeleteSchema() throws Exception {
		// we assume public schema is never deleted
		if (schema.equals(this.sqlgGraph.getSqlDialect().getPublicSchema()) && !preserve){
			return;
		}
		try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
			String A=getLabel(schema,"A");
			String B=getLabel(schema,"B");
			Vertex a = this.sqlgGraph.addVertex(T.label, A, "name", "A");
			Vertex b = this.sqlgGraph.addVertex(T.label, B, "name", "B");
			a.addEdge("E", b,"attr","value");
			
			VertexLabel lbl=this.sqlgGraph.getTopology().getVertexLabel(schema, "A").get();
			lbl.ensureIndexExists(IndexType.UNIQUE,Arrays.asList(lbl.getProperty("name").get()) );
			
			EdgeLabel elbl=lbl.getOutEdgeLabels().values().iterator().next();
			elbl.ensureIndexExists(IndexType.UNIQUE,Arrays.asList(elbl.getProperty("attr").get()) );
			
			Set<PropertyColumn> globalUniqueIndexPropertyColumns = new HashSet<>();
	        globalUniqueIndexPropertyColumns.addAll(new HashSet<>(lbl.getProperties().values()));
	        GlobalUniqueIndex gui1=this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(globalUniqueIndexPropertyColumns);
	        
	        testSchemaBeforeDeletion(this.sqlgGraph,schema,gui1);
	        this.sqlgGraph.tx().commit();
	        testSchemaBeforeDeletion(this.sqlgGraph,schema,gui1);
	        Thread.sleep(1_000);
	        testSchemaBeforeDeletion(sqlgGraph1,schema,gui1);
	        
	        TopologyListenerTest tlt=new TopologyListenerTest();
			this.sqlgGraph.getTopology().registerListener(tlt);
	        
			Schema sch=this.sqlgGraph.getTopology().getSchema(schema).get();
			sch.remove(preserve);
			assertTrue(tlt.receivedEvent(sch, TopologyChangeAction.DELETE));
						
			testSchemaAfterDeletion(this.sqlgGraph,schema,gui1,preserve);
			
			if (rollback){
				this.sqlgGraph.tx().rollback();
				testSchemaBeforeDeletion(this.sqlgGraph,schema,gui1);
		        
			} else {
				TopologyListenerTest tlt1=new TopologyListenerTest();
				sqlgGraph1.getTopology().registerListener(tlt1);
			        
			    this.sqlgGraph.tx().commit();
			    testSchemaAfterDeletion(this.sqlgGraph,schema,gui1,preserve);
			    Thread.sleep(1_000);
			    assertTrue(tlt1.receivedEvent(sch, TopologyChangeAction.DELETE));
			    testSchemaAfterDeletion(sqlgGraph1,schema,gui1,preserve);
			}
		}
	}
	
	private void testSchemaWithOtherEdges(SqlgGraph g,String schema,String otherSchema) throws Exception {
		Optional<Schema> osch=g.getTopology().getSchema(schema);
		assertNotNull(osch);
		assertTrue(osch.isPresent());
		Optional<VertexLabel> ovl=osch.get().getVertexLabel("B");
		assertNotNull(ovl);
		assertTrue(ovl.isPresent());
		
		osch=g.getTopology().getSchema(otherSchema);
		assertNotNull(osch);
		assertTrue(osch.isPresent());
		
		Optional<EdgeLabel> oel=osch.get().getEdgeLabel("E");
		assertNotNull(oel);
		assertTrue(oel.isPresent());
		
		Set<VertexLabel> vls=oel.get().getInVertexLabels();
		assertTrue(vls.contains(ovl.get()));
		
		assertEquals(1L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,otherSchema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
				.out(SQLG_SCHEMA_OUT_EDGES_EDGE).has(SQLG_SCHEMA_EDGE_LABEL_NAME,"E").count().next().longValue());
				
		assertEquals(1L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
					.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"B")
					.out(SQLG_SCHEMA_IN_EDGES_EDGE).has(SQLG_SCHEMA_EDGE_LABEL_NAME,"E").count().next().longValue());
				
		assertTrue(tableExistsInSQL(otherSchema, EDGE_PREFIX+"E"));

	}
	
	private void testSchemaWithOtherEdgesAfterDeletion(SqlgGraph g,String schema,String otherSchema) throws Exception {
		Optional<Schema> osch=g.getTopology().getSchema(schema);
		assertNotNull(osch);
		assertFalse(osch.isPresent());
		
		osch=g.getTopology().getSchema(otherSchema);
		assertNotNull(osch);
		assertTrue(osch.isPresent());
		
		Optional<EdgeLabel> oel=osch.get().getEdgeLabel("E");
		assertNotNull(oel);
		assertFalse(oel.isPresent());
		
		
		assertEquals(0L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,otherSchema)
				.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"A")
				.out(SQLG_SCHEMA_OUT_EDGES_EDGE).has(SQLG_SCHEMA_EDGE_LABEL_NAME,"E").count().next().longValue());
				
		assertEquals(0L,g.topology().V().hasLabel(SQLG_SCHEMA+"."+SQLG_SCHEMA_SCHEMA).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,schema)
					.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(SQLG_SCHEMA_VERTEX_LABEL_NAME,"B")
					.out(SQLG_SCHEMA_IN_EDGES_EDGE).has(SQLG_SCHEMA_EDGE_LABEL_NAME,"E").count().next().longValue());
				
		if (preserve){
			assertTrue(tableExistsInSQL(otherSchema, EDGE_PREFIX+"E"));
		} else {
			assertFalse(tableExistsInSQL(otherSchema, EDGE_PREFIX+"E"));
		}
	}
	
	@Test
	public void testDeleteSchemaWithOtherEdges() throws Exception {
		// we assume public schema is never deleted
		if (schema.equals(this.sqlgGraph.getSqlDialect().getPublicSchema()) && !preserve){
			return;
		}
		try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
			String otherSchema="otherSchema";
			String A=getLabel(otherSchema,"A");
			String B=getLabel(schema,"B");
			Vertex a = this.sqlgGraph.addVertex(T.label, A, "name", "A");
			Vertex b = this.sqlgGraph.addVertex(T.label, B, "name", "B");
			a.addEdge("E", b,"attr","value");
			testSchemaWithOtherEdges(this.sqlgGraph,schema,otherSchema);
			
			this.sqlgGraph.tx().commit();
			testSchemaWithOtherEdges(this.sqlgGraph,schema,otherSchema);
			Thread.sleep(1_000);
			testSchemaWithOtherEdges(sqlgGraph1,schema,otherSchema);
			
			Schema sch=this.sqlgGraph.getTopology().getSchema(schema).get();
			sch.remove(preserve);
			
			testSchemaWithOtherEdgesAfterDeletion(sqlgGraph, schema, otherSchema);
			
			if (rollback){
				this.sqlgGraph.tx().rollback();
				testSchemaWithOtherEdges(this.sqlgGraph,schema,otherSchema);
			} else {
				this.sqlgGraph.tx().commit();
				testSchemaWithOtherEdgesAfterDeletion(sqlgGraph, schema, otherSchema);
				Thread.sleep(1_000);
				testSchemaWithOtherEdgesAfterDeletion(sqlgGraph1, schema, otherSchema);
			}
		}
	}
}
