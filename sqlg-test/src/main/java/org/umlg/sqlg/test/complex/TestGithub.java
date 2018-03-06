package org.umlg.sqlg.test.complex;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Iterator;
import java.util.List;

/**
 * Date: 2016/04/26
 * Time: 5:02 PM
 */
public class TestGithub extends BaseTest {

//    @Test
    public void test() {
        this.sqlgGraph.addVertex("category", "a", "name", "hello");
        this.sqlgGraph.addVertex("category", "b", "name", "ignore");
        this.sqlgGraph.addVertex("category", "a", "name", "world");
        this.sqlgGraph.tx().commit();
        GraphTraversal gt = this.sqlgGraph.traversal().V().group().by("category")
                .unfold()
                .where(__.select(Column.values).count(Scope.local).is(P.gt(1)))
                .select(Column.values)
                .unfold()
                .<String>values("name");
        printTraversalForm(gt);
        List<String> values = gt.toList();
    }

    @Test
    public void edgeUpdate() {
        Vertex a = this.sqlgGraph.addVertex("A");
        Vertex b = this.sqlgGraph.addVertex("B");
        Edge a2b = a.addEdge("a2b", b);
        a2b.property("someKey", "someValue");

        Edge found_a2b = this.sqlgGraph.traversal().E().has("someKey", "someValue").next();
        found_a2b.property("anotherKey", "anotherValue");

        this.sqlgGraph.tx().commit();

        Assert.assertEquals("someValue", found_a2b.property("someKey").value());
        Assert.assertEquals("anotherValue", found_a2b.property("anotherKey").value());
        Assert.assertEquals("someValue", a2b.property("someKey").value());
    }

    @Test
    public void testEdge() {
        Vertex a = sqlgGraph.addVertex(T.label, "A");
        Vertex b = sqlgGraph.addVertex(T.label, "B");
        Vertex c = sqlgGraph.addVertex(T.label, "C");
        Edge e1 = a.addEdge("e", b);
        Edge e2 = b.addEdge("e", c);
        sqlgGraph.tx().commit();
        Iterator<Edge> results = gt
                .V().hasLabel("A")
                .out("e")
                .inE("e");
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(e1, results.next());
    }

    @Test
    public void issue62() {
        Vertex a = sqlgGraph.addVertex(T.label, "a", "p", "a");
        Vertex b = sqlgGraph.addVertex(T.label, "b", "p", "b");
        Vertex c = sqlgGraph.addVertex(T.label, "c", "p", "c");
        a.addEdge("e", b, "p", "x");
        b.addEdge("e", c, "p", "y");
        sqlgGraph.tx().commit();

        //throws exception during query generation
        Iterator<Vertex> results = gt
                .V().has("p", "a")
                .out("e").has("p", "b")
                .outE("e")
                .or(__.has("p", "x"), __.has("p", "y"))
                .inV().has("p", "c");
        Assert.assertEquals(c, results.next());
    }

    @Test
    public void testWhereQuery() {
        Vertex tnt = sqlgGraph.addVertex(T.label, "tenant", "__type", "tenant");
        Vertex env = sqlgGraph.addVertex(T.label, "environment", "__type", "environment");
        Vertex res = sqlgGraph.addVertex(T.label, "resource", "__type", "resource");
        Vertex de = sqlgGraph.addVertex(T.label, "dataEntity", "__type", "dataEntity");
        Vertex dRoot = sqlgGraph.addVertex(T.label, "structuredData", "__type", "structuredData");
        Vertex dPrims = sqlgGraph.addVertex(T.label, "structuredData", "__type", "structuredData", "__structuredDataKey", "primitives");
        Vertex d0 = sqlgGraph.addVertex(T.label, "structuredData", "__type", "structuredData", "__structuredDataIndex", 0);

        tnt.addEdge("contains", env);
        env.addEdge("contains", res);
        res.addEdge("contains", de);
        de.addEdge("hasData", dRoot);
        dRoot.addEdge("contains", dPrims);
        dPrims.addEdge("contains", d0);

        Iterator<Vertex> results = gt.V(res).out("contains").has("__type", "dataEntity")
                .where(__.out("hasData").out("contains").has("__type", "structuredData")
                        .has("__structuredDataKey", "primitives").out("contains").has("__type", "structuredData"));

        Assert.assertEquals(de, results.next());
    }

}
