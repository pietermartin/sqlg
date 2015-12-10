package org.umlg.sqlg.test.schema;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Created by pieter on 2015/12/09.
 */
public class TestSqlgSchema  extends BaseTest {

    @Test
    public void testSqlgSchemaExist() {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().count().next(), 0);
    }

}
