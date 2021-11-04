package org.umlg.sqlg.test.edges;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2015/02/23
 * Time: 11:37 AM
 */
public class TestOutE extends BaseTest {

    @Test
    public void testOutEWithLabels() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("aaa", v2);
        v1.addEdge("bbb", v3);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, vertexTraversal(this.sqlgGraph, v1).outE("aaa").count().next().intValue());
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().has("name", "p").outE("aaa").count().next().intValue());
    }

	@Test
    public void testOutEWithAttributes() throws Exception {
	    Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p1");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p2");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p3");
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p4");
        Vertex v5 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p5");
        
        v1.addEdge("aaa", v2,"real",true);
        v1.addEdge("aaa", v3,"real",false);
        v1.addEdge("aaa", v4,"real",true,"other","one");
        v1.addEdge("aaa", v5,"real",false);
        
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> gt=vertexTraversal(this.sqlgGraph, v1).outE()
               		.where(__.inV().has("name",P.within("p4","p2")))
               		.inV();
        assertEquals(2,gt.count().next().intValue());
        gt.close();
       
	}
	
	@Test
	public void testOutEOrder() {
	    Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p1");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p2");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p3");
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p4");
        Vertex v5 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p5");
        v1.addEdge("e1", v2);
        v2.addEdge("e2", v3);
        v3.addEdge("e3", v4,"sequence",1);
        v3.addEdge("e3", v5,"sequence",2);
        
        
        this.sqlgGraph.tx().commit();
        List<Path> ps=vertexTraversal(this.sqlgGraph, v1)
        	.out("e1").as("v2")
        	.out("e2").as("v3")
        	.outE("e3").order().by("sequence")
        	.inV().as("v4-5").path().toList();
        assertEquals(2,ps.size());
        assertEquals(v4,ps.get(0).get("v4-5"));
        assertEquals(v5,ps.get(1).get("v4-5"));
        
        ps=vertexTraversal(this.sqlgGraph, v1)
            	.out("e1").as("v2")
            	.out("e2").as("v3")
            	.outE("e3").order().by("sequence",Order.desc)
            	.inV().as("v4-5").path().toList();
        assertEquals(2,ps.size());
        assertEquals(v4,ps.get(1).get("v4-5"));
        assertEquals(v5,ps.get(0).get("v4-5"));
            
	}
}
