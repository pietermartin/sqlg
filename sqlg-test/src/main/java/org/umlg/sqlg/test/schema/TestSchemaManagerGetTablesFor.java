package org.umlg.sqlg.test.schema;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Created by pieter on 2015/09/26.
 */
public class TestSchemaManagerGetTablesFor extends BaseTest {

    //this test call SqlgElement.loadProperty which call SchemaManager.getTablesFor
    //if the uncommitted property is not present it will throw a NullPointerException
    @Test
    public void testGetTablesForConsidersUncommitedColumns() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        this.sqlgGraph.tx().commit();
        v1.property("surname", 1);
        Assert.assertEquals(v1, this.sqlgGraph.traversal().V(v1.id()).next());
    }
}
