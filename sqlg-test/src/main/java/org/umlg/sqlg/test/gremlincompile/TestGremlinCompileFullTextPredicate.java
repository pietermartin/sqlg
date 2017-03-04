package org.umlg.sqlg.test.gremlincompile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.umlg.sqlg.predicate.FullText;
import org.umlg.sqlg.structure.SchemaManager;
import org.umlg.sqlg.test.BaseTest;

/**
 * test full text support
 * @author jpmoresmau
 *
 */
public class TestGremlinCompileFullTextPredicate extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        BaseTest.beforeClass();
        if (configuration.getString("jdbc.url").contains("postgresql")) {
            configuration.addProperty("distributed", true);
        }
    }

	
	@Test
	public void testDefaultImplementation(){
		FullText ft=new FullText("",null,true);
		assertTrue(ft.test("a fat cat sat on a mat and ate a fat rat", "cat"));
		assertTrue(ft.test("a fat cat sat on a mat and ate a fat rat", "cat rat"));
		assertFalse(ft.test("a fat cat sat on a mat and ate a fat rat", "cat cow"));
	}
	
	@Test
	public void testDocExamples() throws SQLException {
		 assumeTrue(configuration.getString("jdbc.url").contains("postgresql"));
		 Vertex v0=this.sqlgGraph.addVertex(T.label, "Sentence", "name", "a fat cat sat on a mat and ate a fat rat");
		 Vertex v1=this.sqlgGraph.addVertex(T.label, "Sentence", "name", "fatal error");
		 Vertex v2=this.sqlgGraph.addVertex(T.label, "Sentence", "name", "error is not fatal");
		 
		 
		 this.sqlgGraph.tx().commit();
		 try (Statement s=this.sqlgGraph.tx().getConnection().createStatement();){
			 String create="CREATE INDEX sentence_idx ON \""+SchemaManager.VERTEX_PREFIX+"Sentence\" USING GIN (to_tsvector('english', \"public\".\"V_Sentence\".\"name\"))";
			 s.execute(create);
		 }
		 this.sqlgGraph.tx().commit();
		 
		 List<Vertex> vts=this.sqlgGraph.traversal().V().hasLabel("Sentence").has("name",FullText.fullTextMatch("english", "fat & rat")).toList();
		 assertEquals(1,vts.size());
		 assertTrue(vts.contains(v0));
		 vts=this.sqlgGraph.traversal().V().hasLabel("Sentence").has("name",FullText.fullTextMatch("english", "fat & cow")).toList();
		 assertEquals(0,vts.size());
		 
		 vts=this.sqlgGraph.traversal().V().hasLabel("Sentence").has("name",FullText.fullTextMatch("english", "fatal <-> error")).toList();
		 assertEquals(1,vts.size());
		 assertTrue(vts.contains(v1));
		 
		 vts=this.sqlgGraph.traversal().V().hasLabel("Sentence").has("name",FullText.fullTextMatch("english", "fatal & error")).toList();
		 assertEquals(2,vts.size());
		 assertTrue(vts.contains(v1));
		 assertTrue(vts.contains(v2));
		 
		 vts=this.sqlgGraph.traversal().V().hasLabel("Sentence").has("name",FullText.fullTextMatch("english",true, "fatal error")).toList();
		 assertEquals(2,vts.size());
		 assertTrue(vts.contains(v1));
		 assertTrue(vts.contains(v2));
		 
		 vts=this.sqlgGraph.traversal().V().hasLabel("Sentence").has("name",FullText.fullTextMatch("english", "!cat")).toList();
		 assertEquals(2,vts.size());
		 assertTrue(vts.contains(v1));
		 assertTrue(vts.contains(v2));
	}

	@Test
	public void testDocExamplesWhere() throws SQLException {
		 assumeTrue(configuration.getString("jdbc.url").contains("postgresql"));
		 Vertex v0=this.sqlgGraph.addVertex(T.label, "Sentence", "name", "a fat cat sat on a mat and ate a fat rat");
		 Vertex v1=this.sqlgGraph.addVertex(T.label, "Sentence", "name", "fatal error");
		 Vertex v2=this.sqlgGraph.addVertex(T.label, "Sentence", "name", "error is not fatal");
		 
		 
		 this.sqlgGraph.tx().commit();
		 try (Statement s=this.sqlgGraph.tx().getConnection().createStatement();){
			 String create="CREATE INDEX sentence_idx ON \""+SchemaManager.VERTEX_PREFIX+"Sentence\" USING GIN (to_tsvector('english', name))";
			 s.execute(create);
		 }
		 this.sqlgGraph.tx().commit();
		 
		 List<Vertex> vts=this.sqlgGraph.traversal().V().hasLabel("Sentence").where(FullText.fullTextMatch("english",false,"name", "fat & rat")).toList();
		 assertEquals(1,vts.size());
		 assertTrue(vts.contains(v0));
		 vts=this.sqlgGraph.traversal().V().hasLabel("Sentence").where(FullText.fullTextMatch("english",false,"name", "fat & cow")).toList();
		 assertEquals(0,vts.size());
		 
		 vts=this.sqlgGraph.traversal().V().hasLabel("Sentence").where(FullText.fullTextMatch("english",false,"name", "fatal <-> error")).toList();
		 assertEquals(1,vts.size());
		 assertTrue(vts.contains(v1));
		 
		 vts=this.sqlgGraph.traversal().V().hasLabel("Sentence").where(FullText.fullTextMatch("english",false,"name", "fatal & error")).toList();
		 assertEquals(2,vts.size());
		 assertTrue(vts.contains(v1));
		 assertTrue(vts.contains(v2));
		 
		 vts=this.sqlgGraph.traversal().V().hasLabel("Sentence").as("a").where("a",FullText.fullTextMatch("english",true,"name", "fatal error")).toList();
		 assertEquals(2,vts.size());
		 assertTrue(vts.contains(v1));
		 assertTrue(vts.contains(v2));
		 
		 vts=this.sqlgGraph.traversal().V().hasLabel("Sentence").as("a").where("a",FullText.fullTextMatch("english",false,"name", "!cat")).toList();
		 assertEquals(2,vts.size());
		 assertTrue(vts.contains(v1));
		 assertTrue(vts.contains(v2));
		 /*vts=this.sqlgGraph.traversal().V().hasLabel("Sentence").or(
				 __.where(FullText.fullTextMatch("english",false,"name", "fat & rat")),
				 __.where(FullText.fullTextMatch("english",false,"name", "fatal <-> error"))
				 ).toList();
		 assertEquals(2,vts.size());
		 assertTrue(vts.contains(v0));
		 assertTrue(vts.contains(v1));*/
		 
		 v2.addEdge("testEdge", v0);
		 this.sqlgGraph.tx().commit();
		 vts=this.sqlgGraph.traversal().V().hasLabel("Sentence").has("name","error is not fatal").out("testEdge").where(FullText.fullTextMatch("english",false,"name", "fat & rat")).toList();
		 assertEquals(1,vts.size());
		 assertTrue(vts.contains(v0));
	}
	

	@Test
	public void testConcat() throws SQLException {
		 assumeTrue(configuration.getString("jdbc.url").contains("postgresql"));
		 Vertex v0=this.sqlgGraph.addVertex(T.label, "Sentence", "name1", "a fat cat sat on a","name2","mat and ate a fat rat");
		 this.sqlgGraph.addVertex(T.label, "Sentence", "name1", "fatal error");
		 this.sqlgGraph.addVertex(T.label, "Sentence", "name1", "error is not fatal");
		 
		 
		 this.sqlgGraph.tx().commit();
		 try (Statement s=this.sqlgGraph.tx().getConnection().createStatement();){
			 String create="CREATE INDEX sentence_idx ON \""+SchemaManager.VERTEX_PREFIX+"Sentence\" USING GIN (to_tsvector('english', name1 || ' ' || name2))";
			 s.execute(create);
		 }
		 this.sqlgGraph.tx().commit();
		 
		 List<Vertex> vts=this.sqlgGraph.traversal().V().hasLabel("Sentence").where(FullText.fullTextMatch("english",false,"name1 || ' ' || name2", "fat & rat")).toList();
		 assertEquals(1,vts.size());
		 assertTrue(vts.contains(v0));
	}
	
	@Test
	@Ignore("check manually index is used")
	public void testPerf() throws SQLException {
		assumeTrue(configuration.getString("jdbc.url").contains("postgresql"));
		 Vertex v0=this.sqlgGraph.addVertex(T.label, "Sentence", "name", "a fat cat sat on a mat and ate a fat rat");
		 
		 int LOOPS=10000;
		 for (int a=0;a<LOOPS;a++){
			 this.sqlgGraph.addVertex(T.label, "Sentence", "name", "loop"+a);
			 
		 }
		 
		 this.sqlgGraph.tx().commit();
		 try (Statement s=this.sqlgGraph.tx().getConnection().createStatement();){
			 String create="CREATE INDEX sentence_idx ON \""+SchemaManager.VERTEX_PREFIX+"Sentence\" USING GIN (to_tsvector('english',name))";
			 s.execute(create);
		 }
		 this.sqlgGraph.tx().commit();
		 long t0=System.currentTimeMillis();
		 List<Vertex> vts=this.sqlgGraph.traversal().V().hasLabel("Sentence").has("name",FullText.fullTextMatch("english", "fat & rat")).toList();
		 assertEquals(1,vts.size());
		 assertTrue(vts.contains(v0));
		 long t1=System.currentTimeMillis();
		 long delta=t1-t0;
		 System.out.println("query time:"+delta+"ms");
		 
	}
}
