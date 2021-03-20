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

    //This logic is copied from AbstractLabel.addIdentifier
    @Test
    public void testAbstractLabelAddIdentifierLogic() {
        TreeMap<Integer, String> identifierMap = new TreeMap<>();
        ListOrderedSet<String> identifiers = new ListOrderedSet<>();
        addIdentifier(identifierMap, identifiers, "cmUid", 2);
        addIdentifier(identifierMap, identifiers, "virtualGroupId", 1);
        addIdentifier(identifierMap, identifiers, "virtualGroupParentId", 0);
        Assert.assertEquals("virtualGroupParentId", identifiers.get(0));
        Assert.assertEquals("virtualGroupId", identifiers.get(1));
        Assert.assertEquals("cmUid", identifiers.get(2));
    }

    private void addIdentifier(TreeMap<Integer, String> identifierMap, ListOrderedSet<String> identifiers, String propertyName, int index) {
        identifierMap.put(index, propertyName);
        identifiers.clear();
        for (Integer mapIndex: identifierMap.keySet()) {
            identifiers.add(identifierMap.get(mapIndex));
        }
    }

    @Test
    public void testMultipleIDs() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        aSchema.ensureVertexLabelExist(
                "A",
                new HashMap<>(){{
                    put("uid", PropertyType.varChar(100));
                    put("country", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country")));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        int count = 2;
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
        aSchema.ensureVertexLabelExist(
                "A",
                new HashMap<>(){{
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

    @Test(expected = RuntimeException.class)
    public void testIdAsPrimaryKeyIsUnique() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        aSchema.ensureVertexLabelExist(
                "A",
                new HashMap<>(){{
                    put("uid", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country")));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        List<Vertex> ids = new ArrayList<>();
        int count = 5;
        for (int i = 0; i < count; i++) {
            Vertex v = this.sqlgGraph.addVertex(T.label, "A.A", "uid", "aaa", "country", "SA");
            ids.add(v);
        }
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testIdsOnEdge() {
        @SuppressWarnings("Duplicates")
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new HashMap<>() {{
                    put("name", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new HashMap<>() {{
                    put("name", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("name"))
        );
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new HashMap<>() {{
                    put("uid", PropertyType.varChar(100));
                    put("country", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country")));
        this.sqlgGraph.tx().commit();
        List<Object> edgeIds = new ArrayList<>();
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 100; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a" + i);
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b" + i);
            edgeIds.add(a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString(), "country", "SA"));
        }
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(100, this.sqlgGraph.traversal().E(edgeIds.toArray()).toList().size());
    }
}
