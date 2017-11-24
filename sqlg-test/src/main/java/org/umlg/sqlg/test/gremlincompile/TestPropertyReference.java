package org.umlg.sqlg.test.gremlincompile;

import static org.junit.Assert.assertEquals;
import static org.umlg.sqlg.predicate.PropertyReference.propertyRef;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Test column references
 * @author JP Moresmau
 *
 */
public class TestPropertyReference extends BaseTest {

	@BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }
 
	public TestPropertyReference() {
		
	}
	
	 @Test
	 public void testInt() {
		 Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "johnny", "score",2,"experience",3);
	     Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pietie", "score",2,"experience",2);
	     Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "koosie", "score",2,"experience",1);
	       
	     this.sqlgGraph.tx().commit();
	     
	     assertOneStepOneVertex(this.sqlgGraph.traversal().V().hasLabel("Person").has("score",propertyRef(Compare.eq, "experience"))
	    		 ,v2);
	     assertOneStepOneVertex(this.sqlgGraph.traversal().V().hasLabel("Person").has("score",propertyRef(Compare.lt, "experience"))
	    		 ,v1);
	     assertOneStepOneVertex(this.sqlgGraph.traversal().V().hasLabel("Person").has("score",propertyRef(Compare.gt, "experience"))
	    		 ,v3);
	 }
	 
	 private void assertOneStepOneVertex( GraphTraversal<Vertex, Vertex> gt,Vertex v){
		 DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>)gt;
	     assertEquals(2, traversal.getSteps().size());
	     List<Vertex> vertices = traversal.toList();
	     assertEquals(1, traversal.getSteps().size());
	     assertEquals(1, vertices.size());
	     assertEquals(v, vertices.get(0));
	 }
	 
	 @Test
	 public void testMultiplePath(){
		 Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "johnny", "score",2,"experience",3);
	     Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pietie", "score",2,"experience",2);
	     Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "koosie", "score",2,"experience",1);
	       
	     Vertex v4 = this.sqlgGraph.addVertex(T.label, "Group", "name", "Friends");
	     v4.addEdge("contains", v1);
	     v4.addEdge("contains", v2);
	     v4.addEdge("contains", v3);
	     
	     Vertex v5 = this.sqlgGraph.addVertex(T.label, "Company", "name", "Acme");
	     v5.addEdge("groups", v4);
	     
	     this.sqlgGraph.tx().commit();
	    
	     GraphTraversal<Vertex, Map<String,Object>> traversal =sqlgGraph.traversal()
	    		 .V().hasLabel("Company").as("c").out("groups")
	    		 .as("g").out("contains").has("score",propertyRef(Compare.eq, "experience")).as("p")
	    		 .select("c","p");
	     List<Map<String,Object>> l =traversal.toList();
	     assertEquals(1,l.size());
	     assertEquals(v2,l.get(0).get("p"));
	 }
}
