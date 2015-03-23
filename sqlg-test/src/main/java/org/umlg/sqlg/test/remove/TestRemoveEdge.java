package org.umlg.sqlg.test.remove;

import org.apache.tinkerpop.gremlin.process.traversal.T;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/09/15
 * Time: 1:56 PM
 */
public class TestRemoveEdge extends BaseTest {

    @Test
    public void testRemoveEdgeAndVertex() {
        Vertex dataTypeEntity = this.sqlgGraph.addVertex(T.label, "DataTypeEntity", "name", "marko");
        Vertex date1 = this.sqlgGraph.addVertex("name", "a");
        Vertex date2 = this.sqlgGraph.addVertex("name", "b");
        Vertex date3 = this.sqlgGraph.addVertex("name", "c");
        Edge edge1 = dataTypeEntity.addEdge("address", date1);
        Edge edge2 = dataTypeEntity.addEdge("address", date2);
        Edge edge3 = dataTypeEntity.addEdge("address", date3);
        this.sqlgGraph.tx().commit();
        edge1.remove();
        edge2.remove();
        edge3.remove();
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().count().next().intValue());
    }
}
