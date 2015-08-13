package org.umlg.sqlg.test.mod;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/07/26
 * Time: 4:40 PM
 */
public class TestDeletedVertex extends BaseTest {

    @Test
    public void testDeletedVertex() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pieter");
        this.sqlgGraph.tx().close();
        v1.remove();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.traversal().V(v1.id()).hasNext());
        Assert.assertTrue(this.sqlgGraph.traversal().V(v2.id()).hasNext());
    }

    @Test
    public void testRemoveOutVertexRemovesEdges() {
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex dog = this.sqlgGraph.addVertex(T.label, "Dog", "name", "snowy");
        person.addEdge("friend", dog);
        this.sqlgGraph.tx().commit();
        person.remove();
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().toList().size());
    }

    @Test
    public void testRemoveInVertexRemovesEdges() {
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex dog = this.sqlgGraph.addVertex(T.label, "Dog", "name", "snowy");
        person.addEdge("friend", dog);
        this.sqlgGraph.tx().commit();
        dog.remove();
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().toList().size());
    }

//    @Test
//    public void testPerf() {
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        this.sqlgGraph.tx().batchModeOn();
//        Vertex person = this.sqlgGraph.addVertex(T.label, "Person");
//        for (int i = 0; i < 1000000; i++) {
//            Vertex dog = this.sqlgGraph.addVertex(T.label, "Dog");
//            person.addEdge("friend", dog);
//        }
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println(stopWatch.toString());
//        stopWatch.reset();
//        stopWatch.start();
//        person.remove();
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println(stopWatch.toString());
//    }
}
