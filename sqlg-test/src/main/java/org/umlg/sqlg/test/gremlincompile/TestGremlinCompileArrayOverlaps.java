package org.umlg.sqlg.test.gremlincompile;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.predicate.ArrayOverlaps;
import org.umlg.sqlg.structure.topology.IndexType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

/**
 * Test array overlaps predicate
 */
public class TestGremlinCompileArrayOverlaps extends BaseTest {
    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Test
    public void testDefaultImplementation() {
        ArrayOverlaps<Integer> dummy = new ArrayOverlaps<>(new Integer[] {});
        Assert.assertTrue(dummy.test(new Integer[] {}, new Integer[] {}));
        Assert.assertTrue(dummy.test(new Integer[] {1, 2, 3}, new Integer[] {1}));
        Assert.assertTrue(dummy.test(new Integer[] {1, 2, 3}, new Integer[] {3, 2, 1}));
        Assert.assertTrue(dummy.test(new Integer[] {2, 3, 1}, new Integer[] {1, 4, 9}));

        Assert.assertFalse(dummy.test(new Integer[] {1, 2}, new Integer[] {}));
        Assert.assertFalse(dummy.test(new Integer[] {1, 2, 3}, new Integer[] {4}));
        Assert.assertFalse(dummy.test(new Integer[] {1, 2, 3}, new Integer[] {4, 5}));
    }

    @Test
    public void testHasClause_integer() {
        Assume.assumeTrue(isPostgres());
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Foo", "values", new int[] {1, 2, 3, 4, 5});
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Foo", "values", new int[] {6, 2, 8});
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Foo", "values", new int[] {9});
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "Foo", "values", new int[] {});
        Vertex v5 = this.sqlgGraph.addVertex(T.label, "Foo", "values", new int[] {9, 2});

        VertexLabel fooVertexLabel = this.sqlgGraph.getTopology().getVertexLabel("public", "Foo").get();
        fooVertexLabel.ensureIndexExists(IndexType.GIN, Collections.singletonList(fooVertexLabel.getProperty("values").get()));

        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Foo").has("values", new ArrayOverlaps<>(new Integer[] {2, 10, 0}).getPredicate()).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(v1, v2, v5)));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Foo").has("values", new ArrayOverlaps<>(new Integer[] {9, 90, 900}).getPredicate()).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(v3, v5)));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Foo").has("values", new ArrayOverlaps<>(new Integer[] {2, 8, 100, 0}).getPredicate()).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(v1, v2, v5)));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Foo").has("values", new ArrayOverlaps<>(new Integer[] {1, 7, 9}).getPredicate()).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(v1, v3, v5)));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Foo").has("values", new ArrayOverlaps<>(new Integer[] {10, 7}).getPredicate()).toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testHasClause_string() {
        Assume.assumeTrue(isPostgres());
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Foo", "values", new String[] {"1", "2", "3", "4", "5"});
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Foo", "values", new String[] {"6", "2", "8"});
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Foo", "values", new String[] {"9"});
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "Foo", "values", new String[] {});
        Vertex v5 = this.sqlgGraph.addVertex(T.label, "Foo", "values", new String[] {"9", "2"});

        VertexLabel fooVertexLabel = this.sqlgGraph.getTopology().getVertexLabel("public", "Foo").get();
        fooVertexLabel.ensureIndexExists(IndexType.GIN, Collections.singletonList(fooVertexLabel.getProperty("values").get()));

        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Foo").has("values", new ArrayOverlaps<>(new String[] {"2", "10", "0"}).getPredicate()).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(v1, v2, v5)));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Foo").has("values", new ArrayOverlaps<>(new String[] {"9", "90", "900"}).getPredicate()).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(v3, v5)));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Foo").has("values", new ArrayOverlaps<>(new String[] {"2", "8", "100", "0"}).getPredicate()).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(v1, v2, v5)));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Foo").has("values", new ArrayOverlaps<>(new String[] {"1", "7", "9"}).getPredicate()).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(v1, v3, v5)));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Foo").has("values", new ArrayOverlaps<>(new String[] {"10", "7"}).getPredicate()).toList();
        Assert.assertEquals(0, vertices.size());
    }
}
