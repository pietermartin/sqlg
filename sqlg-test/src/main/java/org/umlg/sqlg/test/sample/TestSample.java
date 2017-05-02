package org.umlg.sqlg.test.sample;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/05/02
 */
public class TestSample extends BaseTest {

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
