package org.umlg.sqlg.test.graph;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * test graph steps in the middle of traversal
 * @author jpmoresmau
 *
 */
public class MidTraversalGraphTest extends BaseTest {

	public MidTraversalGraphTest() {
		
	}
	
	@Test
	public void testMidTraversalV(){
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        GraphTraversal<Vertex, Map<String,Object>> g= this.sqlgGraph.traversal().V().hasLabel("A").as("a").V().hasLabel("B").as("b").select("a", "b");
        List<Map<String,Object>> l =g.toList();
        
        assertEquals(1, l.size());
        Map<String,Object> m=l.get(0);
        assertEquals(2,m.size());
        assertEquals(a1,m.get("a"));
        assertEquals(b1,m.get("b"));
        
        ensureCompiledGraphStep(g,2);
	}

	/**
	 * ensure once we've built the traversal, it contains a RangeGlobalStep
	 * @param g
	 */
	private void ensureCompiledGraphStep(GraphTraversal<?, ?>g, int expectedCount){
		DefaultGraphTraversal<?, ?> dgt=(DefaultGraphTraversal<?, ?>)g;
		int count=0;
		for (Step<?, ?> s:dgt.getSteps()){
			if (s.getClass().getSimpleName().equals("SqlgGraphStepCompiled")){
				count++;
			}
		}
		assertEquals(expectedCount,count);
	}
	
}
