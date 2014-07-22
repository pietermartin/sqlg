package org.umlg.sqlgraph.test;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2014/07/22
 * Time: 10:41 AM
 */
public class TestAddVertexViaMap extends BaseTest {

    @Test
    public void testMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("name1", "p1");
        map.put("name2", "p2");
        map.put("name3", "p3");
        Vertex v1 = this.sqlGraph.addVertex("Person", map);
        this.sqlGraph.tx().commit();
        Vertex v2 = this.sqlGraph.V().<Vertex>has(Element.LABEL, "Person").next();
        Assert.assertEquals(v1, v2);
        Assert.assertEquals("p1", v2.property("name1").value());
        Assert.assertEquals("p2", v2.property("name2").value());
        Assert.assertEquals("p3", v2.property("name3").value());
    }

}
