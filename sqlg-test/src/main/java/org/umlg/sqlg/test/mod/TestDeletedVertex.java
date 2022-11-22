package org.umlg.sqlg.test.mod;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyDefinition;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(TestDeletedVertex.class);

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
                        new HashMap<>() {{
                            put("name", PropertyDefinition.of(PropertyType.varChar(100)));
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
                        new HashMap<>() {{
                            put("name", PropertyDefinition.of(PropertyType.varChar(100)));
                            put("uid", PropertyDefinition.of(PropertyType.varChar(100)));
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
                        new HashMap<>() {{
                            put("uid1", PropertyDefinition.of(PropertyType.varChar(100)));
                            put("uid2", PropertyDefinition.of(PropertyType.varChar(100)));
                            put("name", PropertyDefinition.of(PropertyType.STRING));
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        VertexLabel dogVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Dog",
                        new HashMap<>() {{
                            put("uid1", PropertyDefinition.of(PropertyType.varChar(100)));
                            put("uid2", PropertyDefinition.of(PropertyType.varChar(100)));
                            put("name", PropertyDefinition.of(PropertyType.STRING));
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        personVertexLabel.ensureEdgeLabelExist(
                "friend",
                dogVertexLabel,
                new HashMap<>() {{
                    put("uid", PropertyDefinition.of(PropertyType.varChar(100)));
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
                        new HashMap<>() {{
                            put("uid1", PropertyDefinition.of(PropertyType.varChar(100)));
                            put("uid2", PropertyDefinition.of(PropertyType.varChar(100)));
                            put("name", PropertyDefinition.of(PropertyType.STRING));
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        VertexLabel dogVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Dog",
                        new HashMap<>() {{
                            put("uid1", PropertyDefinition.of(PropertyType.varChar(100)));
                            put("uid2", PropertyDefinition.of(PropertyType.varChar(100)));
                            put("name", PropertyDefinition.of(PropertyType.STRING));
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        personVertexLabel.ensureEdgeLabelExist(
                "friend",
                dogVertexLabel,
                new HashMap<>() {{
                    put("uid", PropertyDefinition.of(PropertyType.varChar(100)));
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

    @Test
    public void testPerf() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person");
        for (int i = 0; i < 1_000_000; i++) {
            Vertex dog = this.sqlgGraph.addVertex(T.label, "Dog");
            person.addEdge("friend", dog);
            if (i % 10_000 == 0) {
                LOGGER.info("added {} of 1 000 000", i);
                this.sqlgGraph.tx().flush();
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.debug(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        person.remove();
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.debug(stopWatch.toString());
    }
}
