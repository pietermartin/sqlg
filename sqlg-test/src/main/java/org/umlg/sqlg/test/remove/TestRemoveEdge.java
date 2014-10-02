package org.umlg.sqlg.test.remove;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
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
        Vertex dataTypeEntity = this.sqlG.addVertex(T.label, "DataTypeEntity", "name", "marko");
        Vertex date1 = this.sqlG.addVertex("name", "a");
        Vertex date2 = this.sqlG.addVertex("name", "b");
        Vertex date3 = this.sqlG.addVertex("name", "c");
        Edge edge1 = dataTypeEntity.addEdge("address", date1);
        Edge edge2 = dataTypeEntity.addEdge("address", date2);
        Edge edge3 = dataTypeEntity.addEdge("address", date3);
        this.sqlG.tx().commit();
        edge1.remove();
        edge2.remove();
        edge3.remove();
        this.sqlG.tx().commit();
        Assert.assertEquals(0, this.sqlG.E().count().next().intValue());
    }
}
