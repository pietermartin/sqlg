package org.umlg.sqlg.test;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2014/07/13
 * Time: 5:08 PM
 */
public class TestGetById extends BaseTest {

    @Test
    public void testGetVertexById() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.traversal().V(marko.id()).next();
        Assert.assertEquals(marko, v);
        v = this.sqlgGraph.traversal().V(john.id()).next();
        Assert.assertEquals(john, v);
        v = this.sqlgGraph.traversal().V(peter.id()).next();
        Assert.assertEquals(peter, v);
    }

    @Test
    public void testGetEdgeById() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Edge friendEdge = marko.addEdge("friend", john);
        Edge familyEdge = marko.addEdge("family", john);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(friendEdge, this.sqlgGraph.traversal().E(friendEdge.id()).next());
        Assert.assertEquals(familyEdge, this.sqlgGraph.traversal().E(familyEdge.id()).next());
    }

    @Test
    public void testByCollectionOfIds() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStreamingBatchMode());
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().streamingWithLockBatchModeOn();
        int count = 1_000;
        List<Object> recordIds = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Vertex v = this.sqlgGraph.addVertex(T.label, "Person");
            recordIds.add(v.id());
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println("insert: " + stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        Assert.assertEquals(count, this.sqlgGraph.traversal().V(recordIds).count().next().intValue());
        stopWatch.stop();
        System.out.println("read 1: " + stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        Assert.assertEquals(count, this.sqlgGraph.traversal().V().hasId(recordIds).count().next().intValue());
        stopWatch.stop();
        System.out.println("read 2: " + stopWatch.toString());
    }
}
