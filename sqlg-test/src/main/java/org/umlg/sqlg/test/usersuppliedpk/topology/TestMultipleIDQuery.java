package org.umlg.sqlg.test.usersuppliedpk.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/04/17
 */
public class TestMultipleIDQuery extends BaseTest {

    @Test
    public void testMultipleIDs() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist(
                "A",
                new HashMap<String, PropertyType>(){{
                    put("uid", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country")));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        int count = 1000;
        List<Vertex> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Vertex v = this.sqlgGraph.addVertex(T.label, "A.A", "uid", UUID.randomUUID().toString(), "country", "SA");
            ids.add(v);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V(ids.toArray()).toList();
        Assert.assertEquals(count, vertices.size());
    }

    @Test
    public void testMultipleIDsNormal() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist(
                "A",
                new HashMap<String, PropertyType>(){{
                    put("uid", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        List<Vertex> ids = new ArrayList<>();
        int count = 5;
        for (int i = 0; i < count; i++) {
            Vertex v = this.sqlgGraph.addVertex(T.label, "A.A", "uid", UUID.randomUUID().toString(), "country", "SA");
            ids.add(v);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V(ids.toArray()).toList();
        Assert.assertEquals(count, vertices.size());
    }
}
