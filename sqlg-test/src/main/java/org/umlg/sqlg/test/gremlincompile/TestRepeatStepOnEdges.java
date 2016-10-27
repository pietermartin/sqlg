package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outV;

/**
 * Date: 2016/10/25
 * Time: 5:04 PM
 */
public class TestRepeatStepOnEdges extends BaseTest {

    @Test
    public void test() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        List<Edge> edges = this.sqlgGraph.traversal().E().repeat(outV().outE()).times(2).emit().toList();
        Assert.assertEquals(2, edges.size());
    }

}
