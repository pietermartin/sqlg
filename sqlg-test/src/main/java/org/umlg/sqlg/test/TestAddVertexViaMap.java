package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
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
        Vertex v1 = this.sqlgGraph.addVertex("Person", map);
        this.sqlgGraph.tx().commit();
        Vertex v2 = this.sqlgGraph.traversal().V().<Vertex>has(T.label, "Person").next();
        Assert.assertEquals(v1, v2);
        Assert.assertEquals("p1", v2.property("name1").value());
        Assert.assertEquals("p2", v2.property("name2").value());
        Assert.assertEquals("p3", v2.property("name3").value());


        Map<String, Object> edgeMap = new HashMap<>();
        edgeMap.put("name1", "p1");
        edgeMap.put("name2", "p2");
        edgeMap.put("name3", "p3");
        Edge e1 = ((SqlgVertex)v1).addEdgeWithMap("e1", v2, edgeMap);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("p1", e1.property("name1").value());
        Assert.assertEquals("p2", e1.property("name2").value());
        Assert.assertEquals("p3", e1.property("name3").value());
    }

    @Test
    public void howToUpdateManyRows() {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "joe");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
        this.sqlgGraph.tx().commit();

        List<Vertex> markos = this.sqlgGraph.traversal().V().has(T.label, "Person").<Vertex>has("name", "marko").toList();
        Assert.assertFalse(markos.isEmpty());
        Assert.assertEquals(1, markos.size());
        markos.get(0).property("name", "marko2");
        this.sqlgGraph.tx().commit();

    }

}
