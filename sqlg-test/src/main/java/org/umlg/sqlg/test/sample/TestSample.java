package org.umlg.sqlg.test.sample;

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Collection;
import java.util.Map;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/05/02
 */
public class TestSample extends BaseTest {

    @Test
    public void g_V_group_byXlabelX_byXbothE_weight_fold_sampleXlocal_5XX() {
        loadModern();
        Traversal<Vertex, Map<Object, Object>> traversal = this.sqlgGraph.traversal().V(new Object[0])
                .group().by(T.label).by(__.bothE(new String[0]).values(new String[]{"weight"})
                        .fold().sample(Scope.local, 5));
        Assert.assertTrue(traversal.hasNext());
        Map<String, Collection<Double>> map = (Map) traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(2L, (long) map.size());
        Assert.assertEquals(4L, (long) ((Collection) map.get("software")).size());
        Assert.assertEquals(5L, (long) ((Collection) map.get("person")).size());
    }

    @Test
    public void g_V_localXoutE_sampleX1X_byXweightXX() {
        loadModern();
        final Traversal<Vertex, Edge> traversal = this.sqlgGraph.traversal()
                .V()
                .local(
                        __.outE().sample(1).by("weight")
                );
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        Assert.assertEquals(3, counter);
        Assert.assertFalse(traversal.hasNext());
    }
}
