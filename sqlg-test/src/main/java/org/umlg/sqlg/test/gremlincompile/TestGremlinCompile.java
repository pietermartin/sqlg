package org.umlg.sqlg.test.gremlincompile;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2015/01/01
 * Time: 4:38 PM
 */
public class TestGremlinCompile extends BaseTest {

    @Test
    public void testOutOut() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        a.addEdge("outB", b);
        b.addEdge("outC", c);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, a.out("outB").out("outC").count().next().intValue());
    }
}
