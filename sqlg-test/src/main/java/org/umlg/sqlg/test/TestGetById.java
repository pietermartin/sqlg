package org.umlg.sqlg.test;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgExceptions;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2014/07/13
 * Time: 5:08 PM
 */
public class TestGetById extends BaseTest {

    @Test
    public void testGetByReferencedId() {
        final Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        final Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Edge e = a1.addEdge("ab", a2);
        this.sqlgGraph.tx().commit();
        Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V(new ReferenceVertex(a1.id()));
        Assert.assertTrue(traversal.hasNext());
        Vertex other = traversal.next();
        Assert.assertEquals(a1, other);
        traversal = this.sqlgGraph.traversal().V(new ReferenceVertex(a2.id()));
        Assert.assertTrue(traversal.hasNext());
        other = traversal.next();
        Assert.assertEquals(a2, other);
        Traversal<Edge, Edge> traversalEdge = this.sqlgGraph.traversal().E(new ReferenceEdge(e));
        Assert.assertTrue(traversalEdge.hasNext());
        Edge otherEdge = traversalEdge.next();
        Assert.assertEquals(e, otherEdge);
    }

    @Test
    public void testFailures() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A");
        Edge e1 = a1.addEdge("ab", a2);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V(a1.id(), a2).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(List.of(a1, a2)));

        try {
            this.sqlgGraph.traversal().V(a1.id(), "lala").toList();
            Assert.fail("Expected failure");
        } catch (SqlgExceptions.InvalidIdException e) {
            //noop
        }
        List<Vertex> vertexList = this.sqlgGraph.traversal().V(e1).toList();
        Assert.assertTrue(vertexList.isEmpty());
    }

    @Test
    public void shouldAllowIdsOfMixedTypes() {
        loadModern();
        final List<Vertex> vertices = this.sqlgGraph.traversal().V().toList();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V(vertices.get(0), vertices.get(1).id()).count().next().intValue());
        Assert.assertEquals(2, this.sqlgGraph.traversal().V(vertices.get(0).id(), vertices.get(1)).count().next().intValue());
    }

    @Test
    public void testGetVertexById() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
        this.sqlgGraph.tx().commit();
        Vertex vMarko = this.sqlgGraph.traversal().V(marko).next();
        Assert.assertEquals(marko, vMarko);
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
//        System.out.println("insert: " + stopWatch);
        stopWatch.reset();
        stopWatch.start();
        Assert.assertEquals(count, this.sqlgGraph.traversal().V(recordIds).count().next().intValue());
        stopWatch.stop();
//        System.out.println("read 1: " + stopWatch);
        stopWatch.reset();
        stopWatch.start();
        Assert.assertEquals(count, this.sqlgGraph.traversal().V().hasId(recordIds).count().next().intValue());
        stopWatch.stop();
//        System.out.println("read 2: " + stopWatch);
    }
}
