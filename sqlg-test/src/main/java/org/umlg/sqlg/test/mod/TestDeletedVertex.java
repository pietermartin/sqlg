package org.umlg.sqlg.test.mod;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

/**
 * Date: 2014/07/26
 * Time: 4:40 PM
 */
public class TestDeletedVertex extends BaseTest {

    @Test
    public void testDeletedVertex() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pieter");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().close();
        v1.remove();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.traversal().V(v1.id()).hasNext());
        Assert.assertTrue(this.sqlgGraph.traversal().V(v2.id()).hasNext());
    }

    @Test
    public void testDeletedVertexUserSuppliedPK() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Person",
                        new HashMap<String, PropertyType>() {{
                            put("name", PropertyType.varChar(100));
                        }},
                        ListOrderedSet.listOrderedSet(Collections.singletonList("name"))
                );
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pieter");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().close();
        v1.remove();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.traversal().V(v1.id()).hasNext());
        Assert.assertTrue(this.sqlgGraph.traversal().V(v2.id()).hasNext());
    }

    @Test
    public void testDeletedVertexUserSuppliedPK_with2PK() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Person",
                        new HashMap<String, PropertyType>() {{
                            put("name", PropertyType.varChar(100));
                            put("uid", PropertyType.varChar(100));
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("name", "uid"))
                );
        String uid1 = UUID.randomUUID().toString();
        String uid2 = UUID.randomUUID().toString();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko", "uid", uid1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pieter", "uid", uid2);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().close();
        v1.remove();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.traversal().V(v1.id()).hasNext());
        Assert.assertTrue(this.sqlgGraph.traversal().V(v2.id()).hasNext());
    }

    @Test
    public void testRemoveOutVertexRemovesEdges() {
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex dog = this.sqlgGraph.addVertex(T.label, "Dog", "name", "snowy");
        person.addEdge("friend", dog);
        this.sqlgGraph.tx().commit();
        person.remove();
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().toList().size());
    }

    @Test
    public void testRemoveOutVertexRemovesEdgesUserSuppliedPK() {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Person",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                            put("uid2", PropertyType.varChar(100));
                            put("name", PropertyType.STRING);
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        VertexLabel dogVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Dog",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                            put("uid2", PropertyType.varChar(100));
                            put("name", PropertyType.STRING);
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        personVertexLabel.ensureEdgeLabelExist(
                "friend",
                dogVertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );

        Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "marko");
        Vertex dog = this.sqlgGraph.addVertex(T.label, "Dog", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "snowy");
        person.addEdge("friend", dog, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();
        person.remove();
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().toList().size());
    }

    @Test
    public void testRemoveInVertexRemovesEdges() {
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex dog = this.sqlgGraph.addVertex(T.label, "Dog", "name", "snowy");
        person.addEdge("friend", dog);
        this.sqlgGraph.tx().commit();
        dog.remove();
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().toList().size());
    }

    @Test
    public void testRemoveInVertexRemovesEdgesUserSuppliedPK() {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Person",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                            put("uid2", PropertyType.varChar(100));
                            put("name", PropertyType.STRING);
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        VertexLabel dogVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Dog",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                            put("uid2", PropertyType.varChar(100));
                            put("name", PropertyType.STRING);
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        personVertexLabel.ensureEdgeLabelExist(
                "friend",
                dogVertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
        );

        Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "marko");
        Vertex dog = this.sqlgGraph.addVertex(T.label, "Dog", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString(), "name", "snowy");
        person.addEdge("friend", dog, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();
        dog.remove();
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().toList().size());
    }

//    @Test
//    public void testPerf() {
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        Vertex person = this.sqlgGraph.addVertex(T.label, "Person");
//        for (int i = 0; i < 1000000; i++) {
//            Vertex dog = this.sqlgGraph.addVertex(T.label, "Dog");
//            person.addEdge("friend", dog);
//        }
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println(stopWatch.toString());
//        stopWatch.reset();
//        stopWatch.start();
//        person.remove();
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println(stopWatch.toString());
//    }
}
