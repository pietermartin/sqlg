package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;

/**
 * Date: 2015/08/17
 * Time: 5:10 PM
 */
public class TestColumnNameTranslation extends BaseTest {

    @Test
    public void testLongName() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "AAAAAAAAAAAAAAAAAAAAAAAAA");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "BBBBBBBBBBBBBBBBBBBBBBBBB");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "BBBBBBBBBBBBBBBBBBBBBBBBB");
        Edge e1= a1.addEdge("aaaaaaaaaaaaaaaaaaaaaa_bbbbbbbbbbbbbbbbbbbb", b1);
        Edge e2= a1.addEdge("aaaaaaaaaaaaaaaaaaaaaa_bbbbbbbbbbbbbbbbbbbb", b2);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "CCCCCCCCCCCCCCCCCCCCCCCCC");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "CCCCCCCCCCCCCCCCCCCCCCCCC");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "CCCCCCCCCCCCCCCCCCCCCCCCC");
        b1.addEdge("bbbbbbbbbbbbbbbbbbbbbb_cccccccccccccccc", c1);
        b1.addEdge("bbbbbbbbbbbbbbbbbbbbbb_cccccccccccccccc", c2);
        b1.addEdge("bbbbbbbbbbbbbbbbbbbbbb_cccccccccccccccc", c3);

        Vertex d1 = this.sqlgGraph.addVertex(T.label, "DDDDDDDDDDDDDDDDDDDDDDDDD");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "DDDDDDDDDDDDDDDDDDDDDDDDD");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "DDDDDDDDDDDDDDDDDDDDDDDDD");

        b1.addEdge("bbbbbbbbbbbbbbbbbbbbbb_dddddddddddddddd", d1);
        b1.addEdge("bbbbbbbbbbbbbbbbbbbbbb_dddddddddddddddd", d2);
        b1.addEdge("bbbbbbbbbbbbbbbbbbbbbb_dddddddddddddddd", d3);

        this.sqlgGraph.tx().commit();
        List<Map<String, Object>> gt = this.sqlgGraph.traversal().V(a1)
                .outE("aaaaaaaaaaaaaaaaaaaaaa_bbbbbbbbbbbbbbbbbbbb").as("ab")
                .inV().as("B")
                .outE().as("bcd")
                .inV().as("CD")
                .select("ab","B","bcd","CD").toList();
        Assert.assertFalse(gt.isEmpty());
    }
}
