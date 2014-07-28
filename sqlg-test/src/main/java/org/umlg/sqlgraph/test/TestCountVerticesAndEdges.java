package org.umlg.sqlgraph.test;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/21
 * Time: 7:25 PM
 */
public class TestCountVerticesAndEdges extends BaseTest {

    @Test
    public void testCountVertices()  {
        this.sqlGraph.addVertex(Element.LABEL, "V1", "name", "v1");
        this.sqlGraph.addVertex(Element.LABEL, "v2", "name", "v2");
        this.sqlGraph.addVertex(Element.LABEL, "v3", "name", "v3");
        this.sqlGraph.addVertex(Element.LABEL, "v1", "name", "v4");
        this.sqlGraph.addVertex(Element.LABEL, "v2", "name", "v5");
        this.sqlGraph.addVertex(Element.LABEL, "v3", "name", "v6");
        this.sqlGraph.tx().commit();
        Assert.assertEquals(6L, this.sqlGraph.countVertices(), 0);
    }

    @Test
    public void testCountEdges() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "v1");
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "v2");
        Vertex v3 = this.sqlGraph.addVertex(Element.LABEL, "v3");
        Vertex v4 = this.sqlGraph.addVertex(Element.LABEL, "v4");
        v1.addEdge("e1", v2);
        v1.addEdge("e2", v3);
        v1.addEdge("e3", v4);
        v2.addEdge("e4", v1);
        v2.addEdge("e5", v3);
        v2.addEdge("e6", v4);
        v3.addEdge("e7", v1);
        v3.addEdge("e8", v2);
        v3.addEdge("e9", v4);
        this.sqlGraph.tx().commit();
        Assert.assertEquals(9L, this.sqlGraph.countEdges(), 0);
    }
}
