package org.umlg.sqlg.test.topology.edgeMultiplicity;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeDefinition;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;

@SuppressWarnings("DuplicatedCode")
public class TestEdgeMultiplicityUnique extends BaseTest {

    @Test
    public void testUniqueOneToMany() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                }});
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                }});
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, 1, true),
                        Multiplicity.of(0, 1, true)
                ));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1L, this.sqlgGraph.traversal().V(a1).out("ab").count().next(), 0);

        try {
            Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
            a1.addEdge("ab", b2);
            Assert.fail("Expected unique constraint exception");
        } catch (RuntimeException e) {
            if (isPostgres()) {
                Assert.assertTrue(e.getMessage().contains("duplicate key value violates unique constraint"));
            }
        }
        this.sqlgGraph.tx().rollback();

        try {
            Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
            a2.addEdge("ab", b1);
            Assert.fail("Expected unique constraint exception");
        } catch (RuntimeException e) {
            if (isPostgres()) {
                Assert.assertTrue(e.getMessage().contains("duplicate key value violates unique constraint"));
            }
        }

        this.sqlgGraph.tx().rollback();
        Assert.assertEquals(1L, this.sqlgGraph.traversal().V(a1).out("ab").count().next(), 0);

        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a2.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1L, this.sqlgGraph.traversal().V(a1).out("ab").count().next(), 0);
        Assert.assertEquals(1L, this.sqlgGraph.traversal().V(a2).out("ab").count().next(), 0);
    }

    @Test
    public void testUniqueOneToManyCompositePK() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("name1", PropertyDefinition.of(PropertyType.varChar(10)));
                    put("name2", PropertyDefinition.of(PropertyType.varChar(10)));
                }},
                ListOrderedSet.listOrderedSet(List.of("name1", "name2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("name1", PropertyDefinition.of(PropertyType.varChar(10)));
                    put("name2", PropertyDefinition.of(PropertyType.varChar(10)));
                }},
                ListOrderedSet.listOrderedSet(List.of("name1", "name2"))
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, 1, true),
                        Multiplicity.of(0, 1, true)
                ));
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a1", "name2", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name1", "b1", "name2", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1L, this.sqlgGraph.traversal().V(a1).out("ab").count().next(), 0);

        try {
            Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name1", "b2", "name2", "b2");
            a1.addEdge("ab", b2);
            Assert.fail("Expected unique constraint exception");
        } catch (RuntimeException e) {
            if (isPostgres()) {
                Assert.assertTrue(e.getMessage().contains("duplicate key value violates unique constraint"));
            }
        }
        this.sqlgGraph.tx().rollback();

        try {
            Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a2", "name2", "a2");
            a2.addEdge("ab", b1);
            Assert.fail("Expected unique constraint exception");
        } catch (RuntimeException e) {
            if (isPostgres()) {
                Assert.assertTrue(e.getMessage().contains("duplicate key value violates unique constraint"));
            }
        }

        this.sqlgGraph.tx().rollback();
        Assert.assertEquals(1L, this.sqlgGraph.traversal().V(a1).out("ab").count().next(), 0);

        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a2", "name2", "a2");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name1", "b2", "name2", "b2");
        a2.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1L, this.sqlgGraph.traversal().V(a1).out("ab").count().next(), 0);
        Assert.assertEquals(1L, this.sqlgGraph.traversal().V(a2).out("ab").count().next(), 0);
    }

    @Test
    public void testUniqueManyToMany() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                }});
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                }});
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1, true),
                        Multiplicity.of(0, -1, true)
                )
        );
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a2.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();

        try {
            a1.addEdge("ab", b1);
            Assert.fail("Expected unique constraint exception");
        } catch (RuntimeException e) {
            if (isPostgres()) {
                Assert.assertTrue(e.getMessage().contains("duplicate key value violates unique constraint"));
            }
        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testUniqueManyToManyCompositePK() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("name1", PropertyDefinition.of(PropertyType.varChar(10)));
                    put("name2", PropertyDefinition.of(PropertyType.varChar(10)));
                }},
                ListOrderedSet.listOrderedSet(List.of("name1", "name2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("name1", PropertyDefinition.of(PropertyType.varChar(10)));
                    put("name2", PropertyDefinition.of(PropertyType.varChar(10)));
                }},
                ListOrderedSet.listOrderedSet(List.of("name1", "name2"))
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1, true),
                        Multiplicity.of(0, -1, true)
                )
        );
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a1", "name2", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a2", "name2", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name1", "b1", "name2", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name1", "b2", "name2", "b2");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a2.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();

        try {
            a1.addEdge("ab", b1);
            Assert.fail("Expected unique constraint exception");
        } catch (RuntimeException e) {
            if (isPostgres()) {
                Assert.assertTrue(e.getMessage().contains("duplicate key value violates unique constraint"));
            }
        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testUniqueManyToManyOutCompositePK() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("name1", PropertyDefinition.of(PropertyType.varChar(10)));
                    put("name2", PropertyDefinition.of(PropertyType.varChar(10)));
                }},
                ListOrderedSet.listOrderedSet(List.of("name1", "name2"))
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("name1", PropertyDefinition.of(PropertyType.varChar(10)));
                    put("name2", PropertyDefinition.of(PropertyType.varChar(10)));
                }}
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1, true),
                        Multiplicity.of(0, -1, true)
                )
        );
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a1", "name2", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a2", "name2", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name1", "b1", "name2", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name1", "b2", "name2", "b2");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a2.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();

        try {
            a1.addEdge("ab", b1);
            Assert.fail("Expected unique constraint exception");
        } catch (RuntimeException e) {
            if (isPostgres()) {
                Assert.assertTrue(e.getMessage().contains("duplicate key value violates unique constraint"));
            }
        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testUniqueManyToManyInCompositePK() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("name1", PropertyDefinition.of(PropertyType.varChar(10)));
                    put("name2", PropertyDefinition.of(PropertyType.varChar(10)));
                }}
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("name1", PropertyDefinition.of(PropertyType.varChar(10)));
                    put("name2", PropertyDefinition.of(PropertyType.varChar(10)));
                }},
                ListOrderedSet.listOrderedSet(List.of("name1", "name2"))
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1, true),
                        Multiplicity.of(0, -1, true)
                )
        );
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a1", "name2", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a2", "name2", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name1", "b1", "name2", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name1", "b2", "name2", "b2");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a2.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();

        try {
            a1.addEdge("ab", b1);
            Assert.fail("Expected unique constraint exception");
        } catch (RuntimeException e) {
            if (isPostgres()) {
                Assert.assertTrue(e.getMessage().contains("duplicate key value violates unique constraint"));
            }
        }
        this.sqlgGraph.tx().rollback();
    }
}
