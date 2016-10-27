package org.umlg.sqlg.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

/**
 * Test range and limit, they should be implemented as the SQL level
 * @author jpmoresmau
 *
 */
public class TestRangeLimit extends BaseTest {

	/**
	 * ensure once we've built the traversal, it contains a RangeGlobalStep
	 * @param g
	 */
	private void ensureRangeGlobal(GraphTraversal<?, ?>g){
		DefaultGraphTraversal<?, ?> dgt=(DefaultGraphTraversal<?, ?>)g;
		boolean found=false;
		for (Step<?, ?> s:dgt.getSteps()){
			found |= (s instanceof RangeGlobalStep<?>);
		}
		assertTrue(found);
	}
	
	
	/**
	 * once we've run the traversal, it shouldn't contain the RangeGlobalStep, 
	 * since it was changed into a Range on the ReplacedStep
	 * @param g
	 */
	private void ensureSQLImplementsRange(GraphTraversal<?, ?>g){
		DefaultGraphTraversal<?, ?> dgt=(DefaultGraphTraversal<?, ?>)g;
		for (Step<?, ?> s:dgt.getSteps()){
			assertFalse(s instanceof RangeGlobalStep<?>);
		}
	}
	
	@Test
	public void testRangeOnVertexLabels(){
		this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex(T.label, "A", "name", "a" + i);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex,Object> g=this.sqlgGraph.traversal().V().hasLabel("A").order().by("name").range(1, 4).values("name");
        ensureRangeGlobal(g);
        int cnt=0;
        Set<String> names=new HashSet<>();
        String previous=null;
        while (g.hasNext()){
        	String n=(String)g.next();
        	names.add(n);
        	if (previous!=null){
        		assertTrue(previous.compareTo(n)<0);
        	}
        	previous=n;
        	cnt++;
        }
        ensureSQLImplementsRange(g);
        assertEquals(3,cnt);
        assertEquals(names.toString(),3,names.size());
        assertTrue(names.toString(),names.contains("a1"));
        assertTrue(names.toString(),names.contains("a10"));
        assertTrue(names.toString(),names.contains("a11"));
        
	}
	
	@Test
	public void testRangeOnVertexLabelsNoOrder(){
		this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex(T.label, "A", "name", "a" + i);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex,Object> g=this.sqlgGraph.traversal().V().hasLabel("A").range(1, 4).values("name");
        ensureRangeGlobal(g);
        int cnt=0;
        Set<String> names=new HashSet<>();
        while (g.hasNext()){
        	String n=(String)g.next();
        	names.add(n);
        	cnt++;
        }
        ensureSQLImplementsRange(g);
        assertEquals(3,cnt);
        assertEquals(names.toString(),3,names.size());
        
	}
	
	@Test
	public void testLimitOnVertexLabels(){
		this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex(T.label, "A", "name", "a" + i);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex,Object> g=this.sqlgGraph.traversal().V().hasLabel("A").order().by("name").limit(3).values("name");
        ensureRangeGlobal(g);
        int cnt=0;
        Set<String> names=new HashSet<>();
        String previous=null;
        while (g.hasNext()){
        	String n=(String)g.next();
        	names.add(n);
        	if (previous!=null){
        		assertTrue(previous.compareTo(n)<0);
        	}
        	previous=n;
        	cnt++;
        }
        ensureSQLImplementsRange(g);
        assertEquals(3,cnt);
        assertEquals(names.toString(),3,names.size());
        assertTrue(names.toString(),names.contains("a1"));
        assertTrue(names.toString(),names.contains("a10"));
        assertTrue(names.toString(),names.contains("a0"));
        
	}
	
	@Test
	public void testRangeOnEdgeLabels(){
		for (int i = 0; i < 100; i++) {
            Vertex a=this.sqlgGraph.addVertex(T.label, "A", "name", "a" + i);
            Vertex b=this.sqlgGraph.addVertex(T.label, "B", "name", "b" + i);
            a.addEdge("E", b, "name", "e" + i);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Edge,Object> g=this.sqlgGraph.traversal().E().hasLabel("E").order().by("name").range(1, 4).values("name");
        ensureRangeGlobal(g);
        int cnt=0;
        Set<String> names=new HashSet<>();
        String previous=null;
        while (g.hasNext()){
        	String n=(String)g.next();
        	names.add(n);
        	if (previous!=null){
        		assertTrue(previous.compareTo(n)<0);
        	}
        	previous=n;
        	cnt++;
        }
        ensureSQLImplementsRange(g);
        assertEquals(3,cnt);
        assertEquals(names.toString(),3,names.size());
        assertTrue(names.toString(),names.contains("e1"));
        assertTrue(names.toString(),names.contains("e10"));
        assertTrue(names.toString(),names.contains("e11"));
        
	}
	
	@Test
	public void testRangeOnEdgesOutput(){
		Vertex a=this.sqlgGraph.addVertex(T.label, "A", "name", "a0");
        
		for (int i = 0; i < 100; i++) {
            Vertex b=this.sqlgGraph.addVertex(T.label, "B", "name", "b" + i);
            a.addEdge("E", b, "name", "e" + i);
        }
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex,Object> g=this.sqlgGraph.traversal().V(a).out("E").order().by("name").range(1, 4).values("name");
        ensureRangeGlobal(g);
        int cnt=0;
        Set<String> names=new HashSet<>();
        String previous=null;
        while (g.hasNext()){
        	String n=(String)g.next();
        	names.add(n);
        	if (previous!=null){
        		assertTrue(previous.compareTo(n)<0);
        	}
        	previous=n;
        	cnt++;
        }
        ensureSQLImplementsRange(g);
        assertEquals(3,cnt);
        assertEquals(names.toString(),3,names.size());
        assertTrue(names.toString(),names.contains("b1"));
        assertTrue(names.toString(),names.contains("b10"));
        assertTrue(names.toString(),names.contains("b11"));
        
	}
}
