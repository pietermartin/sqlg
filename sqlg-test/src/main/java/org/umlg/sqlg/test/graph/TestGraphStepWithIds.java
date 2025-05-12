package org.umlg.sqlg.test.graph;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2015/11/18
 * Time: 8:47 AM
 */
public class TestGraphStepWithIds extends BaseTest {

//    @Test
//    public void testHasIdNull() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
//        this.sqlgGraph.addVertex(T.label, "A");
//        this.sqlgGraph.tx().commit();
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasId(a1.id(), a2.id(), null).toList();
//        Assert.assertEquals(2, vertices.size());
//        Assert.assertEquals(a1, vertices.get(0));
//        Assert.assertEquals(a2, vertices.get(1));
//    }
//
//    @Test
//    public void testGraphWithIds() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
//        this.sqlgGraph.addVertex(T.label, "A");
//        this.sqlgGraph.addVertex(T.label, "A");
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal().V(a1.id()).toList();
//        Assert.assertEquals(1, vertices.size());
//        Assert.assertEquals(a1, vertices.get(0));
//
//        vertices = this.sqlgGraph.traversal().V(a1).toList();
//        Assert.assertEquals(1, vertices.size());
//        Assert.assertEquals(a1, vertices.get(0));
//    }
//
//    @Test
//    public void testGraphWithIdsGroupingOfIdsAccordingToLabel() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Edge e1 = b3.addEdge("bc", c1);
//        this.sqlgGraph.tx().commit();
//        List<Vertex> vertices = this.sqlgGraph.traversal().V(new Object[]{a1, a2, b1, b2, b3}).toList();
//        Assert.assertEquals(5, vertices.size());
//
//        vertices = this.sqlgGraph.traversal().V(new Object[]{a1, a2, b1, b2, b3}).outE().outV().toList();
//        Assert.assertEquals(1, vertices.size());
//
//        List<Object> values =this.sqlgGraph.traversal().E(e1.id()).inV().values("name").toList();
//        Assert.assertEquals(1, values.size());
//    }
//
//    @Test
//    public void shouldNotThrowExceptionIfIdsAreMixed() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        b1.addEdge("bc", c1);
//        this.sqlgGraph.tx().commit();
//        List<Vertex> vertices = this.sqlgGraph.traversal().V(new Object[]{a1,  b1.id()}).toList();
//        Assert.assertEquals(2, vertices.size());
//    }
//
//    @Test
//    public void testIdAndHasLabel() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        this.sqlgGraph.tx().commit();
//        List<Vertex> vertices = this.sqlgGraph.traversal().V(a1).hasLabel("A").toList();
//        Assert.assertEquals(1, vertices.size());
//        Assert.assertTrue(vertices.contains(a1));
//
//    }

    @SuppressWarnings("ConfusingArgumentToVarargsMethod")
    @Test
    public void testGetNullVertex() {
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.tx().commit();
        Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasId(person.id());
        Assert.assertTrue(traversal.hasNext());
        traversal = this.sqlgGraph.traversal().V(null);
        Assert.assertFalse(traversal.hasNext());

        Vertex person2 = sqlgGraph.addVertex(T.label, "Person");
        traversal = sqlgGraph.traversal().V(null, person2.id());
        Assert.assertTrue(traversal.hasNext());
    }

//    @Test
//    public void testGetByNullId() {
//        Vertex person = this.sqlgGraph.addVertex(T.label, "Person");
//        this.sqlgGraph.tx().commit();
//        Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasId(person.id());
//        Assert.assertTrue(traversal.hasNext());
//        Assert.assertEquals(person, traversal.next());
//        traversal = this.sqlgGraph.traversal().V().hasId(null);
//        Assert.assertFalse(traversal.hasNext());
//    }
//
//    @Test
//    public void testGetByReferencedId() {
//        final Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
//        final Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
//        Edge e = a1.addEdge("ab", a2);
//        this.sqlgGraph.tx().commit();
//        Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V(new ReferenceVertex(a1.id()));
//        Assert.assertTrue(traversal.hasNext());
//        Vertex other = traversal.next();
//        Assert.assertEquals(a1, other);
//        traversal = this.sqlgGraph.traversal().V(new ReferenceVertex(a2.id()));
//        Assert.assertTrue(traversal.hasNext());
//        other = traversal.next();
//        Assert.assertEquals(a2, other);
//        Traversal<Edge, Edge> traversalEdge = this.sqlgGraph.traversal().E(new ReferenceEdge(e));
//        Assert.assertTrue(traversalEdge.hasNext());
//        Edge otherEdge = traversalEdge.next();
//        Assert.assertEquals(e, otherEdge);
//    }
//
//    @Test
//    public void testFailures() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
//        this.sqlgGraph.addVertex(T.label, "A");
//        Edge e1 = a1.addEdge("ab", a2);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal().V(a1.id(), a2).toList();
//        Assert.assertEquals(2, vertices.size());
//        Assert.assertTrue(vertices.containsAll(List.of(a1, a2)));
//
//        try {
//            this.sqlgGraph.traversal().V(a1.id(), "lala").toList();
//            Assert.fail("Expected failure");
//        } catch (SqlgExceptions.InvalidIdException e) {
//            //noop
//        }
//        List<Vertex> vertexList = this.sqlgGraph.traversal().V(e1).toList();
//        Assert.assertTrue(vertexList.isEmpty());
//    }
//
//    @Test
//    public void shouldAllowIdsOfMixedTypes() {
//        loadModern();
//        final List<Vertex> vertices = this.sqlgGraph.traversal().V().toList();
//        Assert.assertEquals(2, this.sqlgGraph.traversal().V(vertices.get(0), vertices.get(1).id()).count().next().intValue());
//        Assert.assertEquals(2, this.sqlgGraph.traversal().V(vertices.get(0).id(), vertices.get(1)).count().next().intValue());
//    }
//
//    @Test
//    public void testGetVertexById() {
//        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
//        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
//        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
//        this.sqlgGraph.tx().commit();
//        Vertex vMarko = this.sqlgGraph.traversal().V(marko).next();
//        Assert.assertEquals(marko, vMarko);
//        Vertex v = this.sqlgGraph.traversal().V(marko.id()).next();
//        Assert.assertEquals(marko, v);
//        v = this.sqlgGraph.traversal().V(john.id()).next();
//        Assert.assertEquals(john, v);
//        v = this.sqlgGraph.traversal().V(peter.id()).next();
//        Assert.assertEquals(peter, v);
//    }
//
//    @Test
//    public void testGetEdgeById() {
//        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
//        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
//        Edge friendEdge = marko.addEdge("friend", john);
//        Edge familyEdge = marko.addEdge("family", john);
//        this.sqlgGraph.tx().commit();
//        Assert.assertEquals(friendEdge, this.sqlgGraph.traversal().E(friendEdge.id()).next());
//        Assert.assertEquals(familyEdge, this.sqlgGraph.traversal().E(familyEdge.id()).next());
//    }
//
//    @Test
//    public void testByCollectionOfIds() {
//        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStreamingBatchMode());
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        this.sqlgGraph.tx().streamingWithLockBatchModeOn();
//        int count = 1_000;
//        List<Object> recordIds = new ArrayList<>();
//        for (int i = 0; i < count; i++) {
//            Vertex v = this.sqlgGraph.addVertex(T.label, "Person");
//            recordIds.add(v.id());
//        }
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
////        System.out.println("insert: " + stopWatch);
//        stopWatch.reset();
//        stopWatch.start();
//        Assert.assertEquals(count, this.sqlgGraph.traversal().V(recordIds).count().next().intValue());
//        stopWatch.stop();
////        System.out.println("read 1: " + stopWatch);
//        stopWatch.reset();
//        stopWatch.start();
//        Assert.assertEquals(count, this.sqlgGraph.traversal().V().hasId(recordIds).count().next().intValue());
//        stopWatch.stop();
////        System.out.println("read 2: " + stopWatch);
//    }
}
