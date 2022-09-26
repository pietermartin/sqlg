package org.umlg.sqlg.test.inject;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestInject extends BaseTest {

    @Test
    public void g_VX1X_valuesXageX_injectXnullX() {
        loadModern();
        Object vid1 = this.convertToVertexId("marko");
        Traversal<Vertex, Object> traversal = this.sqlgGraph.traversal().V(vid1).values("age").inject(null);
        this.printTraversalForm(traversal);
        checkResults(Arrays.asList(29, null), traversal);
    }

    @Test
    public void g_injectXnullX() {
        loadModern();
        Traversal<Integer, Integer> traversal =  this.sqlgGraph.traversal().inject(null);
        this.printTraversalForm(traversal);
        checkResults(Collections.singletonList(null), traversal);
    }

    @Test
    public void g_VX1X_valuesXageX_injectXnull_nullX() {
        loadModern();
        Object vid1 = convertToVertexId("marko");
        final Traversal<Vertex, Object> traversal =  this.sqlgGraph.traversal().V(vid1).values("age").inject(null, null);
        printTraversalForm(traversal);
        checkResults(Arrays.asList(29, null, null), traversal);
    }

    @Test
    public void g_injectXnull_nullX() {
        loadModern();
        final Traversal<Integer, Integer> traversal =  this.sqlgGraph.traversal().inject(null, null);
        printTraversalForm(traversal);
        checkResults(Arrays.asList(null, null), traversal);
    }

    @Test
    public void g_injectX1_null_nullX_path() {
        loadModern();
        Traversal<Integer, Path> traversal =  this.sqlgGraph.traversal().inject(new Integer[]{1, null, null}).path();
        this.printTraversalForm(traversal);
        checkResults(Arrays.asList(MutablePath.make().extend(1, Collections.emptySet()), MutablePath.make().extend((Object)null, Collections.emptySet()), MutablePath.make().extend((Object)null, Collections.emptySet())), traversal);
    }

    @Test
    public void testInjectWithChoose() {
        Traversal<?, ?> traversal = this.sqlgGraph.traversal().inject(1).choose(__.is(1), __.constant(10).fold(), __.fold());
        final List<Integer> expected = new ArrayList<>() {{
            add(10);
        }};
        Assert.assertEquals(expected, traversal.next());
        MatcherAssert.assertThat(traversal.hasNext(), Matchers.is(false));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInjectAndUnion() {
        this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().inject("ignore").union(__.V().hasLabel("A"), __.V().hasLabel("A")).toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testInjectNestedBranchTraversalStepBug() {
        List<String> result = this.sqlgGraph.traversal()
                .inject("test1", "test2")
                .where(
                        __.choose(
                                __.V().hasLabel("DoesntMatter"),
                                __.constant(true),
                                __.constant(false)
                        )
                ).toList();
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("test1", result.get(0));
        Assert.assertEquals("test2", result.get(1));
    }
}
