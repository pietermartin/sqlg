package org.umlg.sqlg.test.reducing;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Map;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/05/02
 */
public class TestMax extends BaseTest {

    @Test
    public void g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_maxX() {
        loadModern();
        final Traversal<Vertex, Map<String, Number>> traversal = this.sqlgGraph.traversal()
                .V().hasLabel("software")
                .<String, Number>group().by("name").by(
                        __.bothE().values("weight").max()
                );
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        final Map<String, Number> map = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(2, map.size());
        Assert.assertEquals(1.0, map.get("ripple"));
        Assert.assertEquals(0.4, map.get("lop"));
    }
}
