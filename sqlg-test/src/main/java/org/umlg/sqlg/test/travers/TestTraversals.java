package org.umlg.sqlg.test.travers;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


/**
 * Date: 2015/03/28
 * Time: 9:58 AM
 */
public class TestTraversals extends BaseTest {

    @Test
    public void g_V_both_both_count() {
        loadModern();
        final Traversal<Vertex, Long> traversal = this.sqlgGraph.traversal().V().both().both().count();
        printTraversalForm(traversal);
        assertEquals(new Long(30), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    public void g_VX1X_out_hasXid_2X() {
        loadModern();
        final Traversal<Vertex, Vertex> traversal = this.gt.V(convertToVertexId("marko")).out().hasId(convertToVertexId("vadas"));
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        Assert.assertEquals(convertToVertexId("vadas"), traversal.next().id());
    }

    @Test
    public void g_V_out_out_treeXaX_capXaX() {
        loadModern();
        Traversal t1 = gt.V().out().out().tree();
        Traversal t2 = gt.V().out().out().tree("a").cap("a");
        @SuppressWarnings("unchecked") List<Traversal<Vertex, Tree>> traversals = Arrays.asList(t1, t2);
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Tree tree = traversal.next();
            assertFalse(traversal.hasNext());
            assertEquals(1, tree.size());
            Assert.assertTrue(tree.containsKey(convertToVertex(this.sqlgGraph, "marko")));
            assertEquals(1, ((Map) tree.get(convertToVertex(this.sqlgGraph, "marko"))).size());
            Assert.assertTrue(((Map) tree.get(convertToVertex(this.sqlgGraph, "marko"))).containsKey(convertToVertex(this.sqlgGraph, "josh")));
            Assert.assertTrue(((Map) ((Map) tree.get(convertToVertex(this.sqlgGraph, "marko"))).get(convertToVertex(this.sqlgGraph, "josh"))).containsKey(convertToVertex(this.sqlgGraph, "lop")));
            Assert.assertTrue(((Map) ((Map) tree.get(convertToVertex(this.sqlgGraph, "marko"))).get(convertToVertex(this.sqlgGraph, "josh"))).containsKey(convertToVertex(this.sqlgGraph, "ripple")));
        });
    }

    @Test
    public void g_V_out_out_path_byXnameX_byXageX() {
        loadModern();
        gt.V().out().out().toList();
        gt.V().out().out().path().toList();
        final Traversal<Vertex, Path> traversal =  gt.V().out().out().path().by("name").by("age");
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Path path = traversal.next();
            Assert.assertEquals(3, path.size());
            Assert.assertEquals("marko", path.<String>get(0));
            Assert.assertEquals(Integer.valueOf(32), path.<Integer>get(1));
            Assert.assertTrue(path.get(2).equals("lop") || path.get(2).equals("ripple"));
        }
        Assert.assertEquals(2, counter);
    }
}
