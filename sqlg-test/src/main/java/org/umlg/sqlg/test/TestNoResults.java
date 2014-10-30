package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Date: 2014/08/06
 * Time: 11:29 AM
 */
public class TestNoResults extends BaseTest {

    @Test
    public void testNoResult() {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.V().<Vertex>has(T.label, "Person").next();
        Assert.assertEquals("John", v.property("name").value());
        this.sqlgGraph.V().remove();
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.V().count().next(), 0);
        Assert.assertEquals(0, this.sqlgGraph.V().has(T.label, "Person").count().next(), 0);

        Set<Long> result = new HashSet<>();
        this.sqlgGraph.V().<Vertex>has(T.label, "Person").forEachRemaining (
                vertex -> result.add((Long)vertex.id())
        );
        Assert.assertEquals(0, result.size());
    }
}
