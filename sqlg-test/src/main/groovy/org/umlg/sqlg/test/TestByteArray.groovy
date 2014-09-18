package org.umlg.sqlg.test

import com.tinkerpop.gremlin.structure.Vertex
import org.junit.Assert
import org.junit.Test
import com.tinkerpop.gremlin.process.T

/**
 * Created by pieter on 2014/08/03.
 */
class TestByteArray extends BaseTest {

    @Test
    public void testByteArray() {
        byte[] bytea = [1, 2, 3, 4, 5, 6, 7, 8, 9];
        Vertex v1 = this.sqlG.addVertex("Person", [name: 'pieter', bytea: bytea]);
        this.sqlG.tx().commit()

        Vertex v2 = this.sqlG.V().has(T.label, "Person").next();
        Assert.assertEquals(v1, v2);
        Assert.assertArrayEquals(bytea, v2.property("bytea").value());
    }
}
