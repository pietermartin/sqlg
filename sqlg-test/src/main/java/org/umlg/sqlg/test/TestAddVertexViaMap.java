package org.umlg.sqlg.test;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.structure.topology.VertexLabel;

import java.util.*;

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
        Edge e1 = ((SqlgVertex) v1).addEdgeWithMap("e1", v2, edgeMap);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("p1", e1.property("name1").value());
        Assert.assertEquals("p2", e1.property("name2").value());
        Assert.assertEquals("p3", e1.property("name3").value());
    }

    @Test
    public void testMapUserSuppliedPK() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Person",
                        new HashMap<String, PropertyType>() {{
                            put("uid", PropertyType.varChar(100));
                            put("name1", PropertyType.STRING);
                            put("name2", PropertyType.STRING);
                            put("name3", PropertyType.STRING);
                        }},
                        ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
                );
        Map<String, Object> map = new HashMap<>();
        map.put("uid", UUID.randomUUID().toString());
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


        vertexLabel.ensureEdgeLabelExist(
                "e1",
                vertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.varChar(100));
                    put("name1", PropertyType.STRING);
                    put("name2", PropertyType.STRING);
                    put("name3", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );

        Map<String, Object> edgeMap = new HashMap<>();
        edgeMap.put("uid", UUID.randomUUID().toString());
        edgeMap.put("name1", "p1");
        edgeMap.put("name2", "p2");
        edgeMap.put("name3", "p3");
        Edge e1 = ((SqlgVertex) v1).addEdgeWithMap("e1", v2, edgeMap);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("p1", e1.property("name1").value());
        Assert.assertEquals("p2", e1.property("name2").value());
        Assert.assertEquals("p3", e1.property("name3").value());
    }


}
