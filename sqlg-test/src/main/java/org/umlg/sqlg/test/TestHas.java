package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertFalse;

/**
 * Date: 2014/07/13
 * Time: 6:36 PM
 */
@SuppressWarnings({"DuplicatedCode", "unused"})
public class TestHas extends BaseTest {

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_bothE_properties_dedup_hasKeyXweightX_hasValueXltX0d3XX_value() {
        loadModern();
        final Traversal<Vertex, Double> traversal = this.sqlgGraph.traversal()
                .V()
                .bothE()
                .properties().dedup().hasKey("weight").hasValue(P.lt(0.3)).value();
        printTraversalForm(traversal);
        checkResults(Collections.singletonList(0.2), traversal);
    }

    @Test
    public void g_V_hasXblahX() {
        loadModern();
        final Traversal<Vertex, Vertex> traversal =  this.sqlgGraph.traversal().V().has("blah");
        printTraversalForm(traversal);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testHasProperty() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().has("name").count().next(), 0);
        Assert.assertTrue(this.sqlgGraph.traversal().V().has("name").toList().containsAll(Arrays.asList(a1, a2)));
    }

    @Test
    public void testHasNotProperty() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasNot("name").count().next(), 0);
        Assert.assertTrue(this.sqlgGraph.traversal().V().hasNot("name").toList().containsAll(Arrays.asList(a3, a4)));
    }

    @Test
    public void g_V_in_hasIdXneqX1XX() {
        loadModern();
        Object marko = convertToVertex(this.sqlgGraph, "marko").id();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().in().hasId(P.neq(marko)).toList();
        int count = 0;
        for (Vertex vertex : vertices) {
            Assert.assertTrue(vertex.value("name").equals("josh") || vertex.value("name").equals("peter"));
            count++;
        }
        Assert.assertEquals(3, count);
    }

    @Test
    public void testHasNotId() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().in().hasId(P.neq(a1.id())).toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void g_V_hasId() {
        loadModern();
        assertModernGraph(this.sqlgGraph, true, false);
        GraphTraversalSource g = this.sqlgGraph.traversal();

        Object id = convertToVertexId("marko");

        List<Vertex> traversala2 =  g.V().has(T.id, id).toList();
        Assert.assertEquals(1, traversala2.size());
        Assert.assertEquals(convertToVertex(this.sqlgGraph, "marko"), traversala2.get(0));

        traversala2 =  g.V().hasId(id).toList();
        Assert.assertEquals(1, traversala2.size());
        Assert.assertEquals(convertToVertex(this.sqlgGraph, "marko"), traversala2.get(0));
    }

    @Test
    public void testHasId() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasId(a1.id()).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(a1, vertices.get(0));
    }

	@Test
	public void testHasIDDifferentLabels() {
		Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
		Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
		Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
		Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");

		Edge e1 = a1.addEdge("ab", b1);

		List<Object> edges = this.sqlgGraph.traversal().V(a1.id()).outE("ab").as("e").inV().hasId(b1.id()).select("e")
				.toList();
		Assert.assertEquals(1, edges.size());
		Assert.assertEquals(e1, edges.get(0));

		edges = this.sqlgGraph.traversal().V(a1.id()).outE("ab").as("e").inV().hasId(a1.id(),b1.id()).select("e")
				.toList();
		Assert.assertEquals(1, edges.size());
		Assert.assertEquals(e1, edges.get(0));

		edges = this.sqlgGraph.traversal().V(a1.id()).outE("ab").as("e").inV().hasId(P.within(b1.id())).select("e")
				.toList();
		Assert.assertEquals(1, edges.size());
		Assert.assertEquals(e1, edges.get(0));

		edges = this.sqlgGraph.traversal().V(a1.id()).outE("ab").as("e").inV().hasId(c1.id()).select("e")
				.toList();
		Assert.assertEquals(0, edges.size());

		edges = this.sqlgGraph.traversal().V(a1.id()).outE("ab").as("e").inV().hasId(P.within(c1.id())).select("e")
				.toList();
		Assert.assertEquals(0, edges.size());

		edges = this.sqlgGraph.traversal().V(a1.id()).outE("ab").as("e").inV().hasId(P.within(a1.id(),c1.id())).select("e")
				.toList();
		Assert.assertEquals(0, edges.size());

		edges = this.sqlgGraph.traversal().V(a1.id()).outE("ab").as("e").inV().hasId(P.within(a1.id(),b2.id())).select("e")
				.toList();
		Assert.assertEquals(0, edges.size());
	}

    @Test
    public void testHas() {
        this.sqlgGraph.addVertex("name", "marko");
        this.sqlgGraph.addVertex("name", "peter");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void testQueryTableNotYetExists() {
        this.sqlgGraph.addVertex(T.label, "Animal");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
    }

    @Test
    public void testQueryPropertyNotYetExists() {
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Person").has("name", "john").count().next(), 0);
    }

    @Test
    public void testHasOnEdge() {
        Vertex v1 = this.sqlgGraph.addVertex("name", "marko");
        Vertex v2 = this.sqlgGraph.addVertex("name", "peter");
        v1.addEdge("friend", v2, "weight", "5");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().has("weight", "5").count().next(), 0);
    }

    //This is from TinkerPop process test suite. It fails for MariaDb
    @Test
    public void g_V_hasXk_withinXcXX_valuesXkX() {
        Assume.assumeTrue(!isMariaDb());
        this.sqlgGraph.traversal().addV().property("k", "轉注").
                addV().property("k", "✦").
                addV().property("k", "♠").
                addV().property("k", "A").iterate();

        final Traversal<Vertex, String> traversal = this.sqlgGraph.traversal().V().has("k", P.within("轉注", "✦", "♠")).values("k");
        printTraversalForm(traversal);
        checkResults(Arrays.asList("轉注", "✦", "♠"), traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    public void testEdgeQueryTableNotYetExists() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Animal");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Animal");
        v1.addEdge("friend", v2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().has(T.label, "friendXXX").count().next(), 0);
    }

    @Test
    public void testEdgeQueryPropertyNotYetExists() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "friend").has("weight", "5").count().next(), 0);
    }

    @Test
    public void testHasOnTableThatDoesNotExist() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "friend").has("weight", "5").count().next(), 0);
        Assert.assertFalse(this.sqlgGraph.traversal().V().has(T.label, "xxx").hasNext());
        Assert.assertFalse(this.sqlgGraph.traversal().V().has(T.label, "public.xxx").hasNext());
    }

    @Test
    public void testHasOnEmptyProperty() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "something");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "");
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().has(T.label, "Person").has("name").toList();
        Assert.assertEquals(2, vertices.size());
        vertices = this.sqlgGraph.traversal().V().has(T.label, "Person").has("name").has("name", P.neq("")).toList();
        Assert.assertEquals(1, vertices.size());
    }

}
