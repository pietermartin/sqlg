package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.structure.Element;
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
        this.sqlG.addVertex(Element.LABEL, "Person", "name", "John");
        this.sqlG.tx().commit();
        Vertex v = this.sqlG.V().<Vertex>has(Element.LABEL, "Person").next();
        Assert.assertEquals("John", v.property("name").value());
        this.sqlG.V().remove();
        this.sqlG.tx().commit();
        Assert.assertEquals(0, this.sqlG.V().count().next(), 0);
        Assert.assertEquals(0, this.sqlG.V().has(Element.LABEL, "Person").count().next(), 0);

        Set<Long> result = new HashSet<>();
        this.sqlG.V().<Vertex>has(Element.LABEL, "Person").forEach (
                vertex -> result.add((Long)vertex.id())
        );
        Assert.assertEquals(0, result.size());
    }
}
