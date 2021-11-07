package org.umlg.sqlg.test.inject;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.List;

public class TestInject extends BaseTest {

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
