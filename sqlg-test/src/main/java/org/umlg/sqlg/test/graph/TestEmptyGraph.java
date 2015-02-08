package org.umlg.sqlg.test.graph;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2015/02/03
 * Time: 11:02 AM
 */
public class TestEmptyGraph extends BaseTest {

    @Test
    public void testQueryEmptyGraph() {
        Vertex root = this.sqlgGraph.addVertex(T.label, "Root");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0,root.outE("lala").count().next().intValue());
    }
}
