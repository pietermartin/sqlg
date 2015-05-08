package org.umlg.sqlg.test.edges;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2015/02/23
 * Time: 11:37 AM
 */
public class TestOutE extends BaseTest {

    @Test
    public void testOutEWithLabels() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("aaa", v2);
        v1.addEdge("bbb", v3);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, vertexTraversal(v1).outE("aaa").count().next().intValue());
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().has("name", "p").outE("aaa").count().next().intValue());
    }

}
