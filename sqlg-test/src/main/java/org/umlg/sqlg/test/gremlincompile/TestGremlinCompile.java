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
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C", "nAmE", "c");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "NAME", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "NAME", "d2");
        a.addEdge("outB", b);
        b.addEdge("outC", c);
        b.addEdge("outC", c);
        b.addEdge("outD", d1);
//        b.addEdge("outD", d2);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, a.out().out().count().next().intValue());
    }
}
