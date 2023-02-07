package org.umlg.sqlg.test.ltree;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestPostgresLtree extends BaseTest {

    @Test
    public void testLTree() {
        Assume.assumeTrue(isPostgres());
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("Tree", new HashMap<>() {{
                    put("path", PropertyDefinition.of(PropertyType.LTREE));
                }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Tree", "path", "one");
        this.sqlgGraph.addVertex(T.label, "Tree", "path", "one.two");
        this.sqlgGraph.addVertex(T.label, "Tree", "path", "one.two.three");
        this.sqlgGraph.tx().commit();
        List<Vertex> tree = this.sqlgGraph.traversal().V().hasLabel("Tree").toList();
        Assert.assertEquals(3, tree.size());
        Set<String> paths = new HashSet<>(Set.of("one", "one.two", "one.two.three"));
        for (Vertex t: tree) {
            String path = t.value("path");
            paths.remove(path);
        }
        Assert.assertTrue(paths.isEmpty());
    }
}
