package org.umlg.sqlg.test.preparedStatement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2016/05/15
 * Time: 2:49 PM
 */
public class TestClosingPreparedStatement extends BaseTest {

//    @Test
//    public void testClosingPreparedStatement() {
//        this.sqlgGraph.addVertex(T.label, "A");
//        this.sqlgGraph.addVertex(T.label, "A");
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.traversal().V().next();
//        this.sqlgGraph.tx().commit();
//        assertTrue(this.sqlgGraph.tx().getPreparedStatementCache().isEmpty());
//        this.sqlgGraph.traversal().V().next();
//        this.sqlgGraph.tx().rollback();
//        assertTrue(this.sqlgGraph.tx().getPreparedStatementCache().isEmpty());
//    }

    @Test
    public void testAddCommit() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for (int i = 0; i < 100_000; i++) {
            this.sqlgGraph.addVertex(T.label, "A");
            if (i % 10_000 == 0) {
                System.out.println(i);
                this.sqlgGraph.addVertex(T.label, "A", "name" + i, "asda");
            }
            if (i % 1000 == 0) {
                this.sqlgGraph.tx().commit();
                assertTrue(this.sqlgGraph.tx().getPreparedStatementCache().isEmpty());
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }
    
    @Test
    public void testRead() {
    	 Vertex v=this.sqlgGraph.addVertex(T.label, "A");
    	 this.sqlgGraph.tx().commit();
	     GraphTraversal<Vertex, Vertex> gt = sqlgGraph.traversal().V(v.id());
         try {
        	 Vertex v2= gt.next();
        	 assertEquals(v.id(),v2.id());
        	 assertFalse(this.sqlgGraph.tx().getPreparedStatementCache().isEmpty());
        	 assertFalse(gt.hasNext());
        	 assertTrue(this.sqlgGraph.tx().getPreparedStatementCache().isEmpty());
         } finally {
               try {
                     gt.close(); 
                     gt = null;
               } catch (Exception e) {
            	   e.printStackTrace();
            	   fail(e.getMessage());
               }
         }

    	 
    }
}
