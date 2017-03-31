package org.umlg.sqlg.test.localvertexstep;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2017/03/29
 * Time: 10:33 PM
 */
public class TestLocalVertexStepLimit extends BaseTest {

    @Test
    public void g_V_localXoutE_limitX1X_inVX_limitX3X() {
        loadModern();
        final Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal()
                .V().local(
                        __.outE().limit(1)
                ).inV().limit(3);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        Assert.assertEquals(3, counter);
    }
}
