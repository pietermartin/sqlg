package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgVertex;

import java.util.HashMap;
import java.util.List;
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
        Vertex v1 = this.sqlG.addVertex("Person", map);
        this.sqlG.tx().commit();
        Vertex v2 = this.sqlG.V().<Vertex>has(Element.LABEL, "Person").next();
        Assert.assertEquals(v1, v2);
        Assert.assertEquals("p1", v2.property("name1").value());
        Assert.assertEquals("p2", v2.property("name2").value());
        Assert.assertEquals("p3", v2.property("name3").value());


        Map<String, Object> edgeMap = new HashMap<>();
        edgeMap.put("name1", "p1");
        edgeMap.put("name2", "p2");
        edgeMap.put("name3", "p3");
        Edge e1 = ((SqlgVertex)v1).addEdgeWithMap("e1", v2, edgeMap);
        this.sqlG.tx().commit();
        Assert.assertEquals("p1", e1.property("name1").value());
        Assert.assertEquals("p2", e1.property("name2").value());
        Assert.assertEquals("p3", e1.property("name3").value());
    }

    @Test
    public void howToUpdateManyRows() {
        this.sqlG.addVertex(Element.LABEL, "Person", "name", "marko");
        this.sqlG.addVertex(Element.LABEL, "Person", "name", "joe");
        this.sqlG.addVertex(Element.LABEL, "Person", "name", "john");
        this.sqlG.addVertex(Element.LABEL, "Person", "name", "peter");
        this.sqlG.tx().commit();

        List<Vertex> markos = this.sqlG.V().has(Element.LABEL, "Person").<Vertex>has("name", "marko").toList();
        if (markos.isEmpty()) {
            markos.get(0).property("name", "marko2");
        }
        this.sqlG.tx().commit();

    }

}
