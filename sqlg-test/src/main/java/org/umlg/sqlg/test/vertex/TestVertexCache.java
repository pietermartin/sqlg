package org.umlg.sqlg.test.vertex;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/10/04
 * Time: 2:03 PM
 */
public class TestVertexCache extends BaseTest {

    @Test
    public void testVertexTransactionalCache() {
        Vertex v1 = this.sqlG.addVertex(T.label, "Person");
        Vertex v2 = this.sqlG.addVertex(T.label, "Person");
        Vertex v3 = this.sqlG.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);
        Assert.assertEquals(1, v1.out("friend").count().next().intValue());
        Vertex tmpV1 = this.sqlG.v(v1.id());
        tmpV1.addEdge("foe", v3);
        //this should fail as v1's out edges will not be updated
        Assert.assertEquals(1, tmpV1.out("foe").count().next().intValue());
        Assert.assertEquals(1, v1.out("foe").count().next().intValue());
        this.sqlG.tx().rollback();
    }

}
