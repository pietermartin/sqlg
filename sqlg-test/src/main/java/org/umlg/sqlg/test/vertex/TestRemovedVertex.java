package org.umlg.sqlg.test.vertex;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/12/06
 * Time: 9:52 AM
 */
public class TestRemovedVertex extends BaseTest {

    @Test(expected = IllegalStateException.class)
    public void testExceptionOnRemovedVertex() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        v1.remove();
        v1.property("name", "peter");
        Assert.fail("should have thrown exception");
    }
}
