package org.umlg.sqlg.test.topology.edgeMultiplicity;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeDefinition;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@SuppressWarnings("DuplicatedCode")
public class TestMultiplicityAddRemoveEdgeUserDefinedID extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestMultiplicityAddRemoveEdgeUserDefinedID.class);

    @Test
    public void testMultiplicityWithCount_1() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("id1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("id2", PropertyDefinition.of(PropertyType.INTEGER));
                }},
                ListOrderedSet.listOrderedSet(List.of("id1", "id2"))
        );
        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("di1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("di2", PropertyDefinition.of(PropertyType.INTEGER));
                }},
                ListOrderedSet.listOrderedSet(List.of("di1", "di2"))
        );

        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(1, 5))
        );
        this.sqlgGraph.tx().commit();

        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "id1", 1, "id2", 1);
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
            LOGGER.debug("test multiplicity messaging", e);
        }
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "di1", 1, "di2", 1);
        a.addEdge("ab", b);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);

        a.addEdge("ab", b);
        a.addEdge("ab", b);
        a.addEdge("ab", b);
        a.addEdge("ab", b);
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);

        this.sqlgGraph.tx().commit();
        a.addEdge("ab", b);
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testMultiplicityWithCount_2() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("id1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("id2", PropertyDefinition.of(PropertyType.INTEGER));
                }},
                ListOrderedSet.listOrderedSet(List.of("id1", "id2"))
        );
        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B");

        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(1, 5))
        );
        this.sqlgGraph.tx().commit();

        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "id1", 1, "id2", 1);
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
            LOGGER.debug("test multiplicity messaging", e);
        }
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "id1", 1, "id2", 1);
        a.addEdge("ab", b);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);

        a.addEdge("ab", b);
        a.addEdge("ab", b);
        a.addEdge("ab", b);
        a.addEdge("ab", b);
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);

        this.sqlgGraph.tx().commit();
        a.addEdge("ab", b);
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testMultiplicityWithCount_3() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("id1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("id2", PropertyDefinition.of(PropertyType.INTEGER));
                }},
                ListOrderedSet.listOrderedSet(List.of("id1", "id2"))
        );

        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(1, 5))
        );
        this.sqlgGraph.tx().commit();

        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "id1", 1, "id2", 1);
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
            LOGGER.debug("test multiplicity messaging", e);
        }
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "id1", 1, "id2", 1);
        a.addEdge("ab", b);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);

        a.addEdge("ab", b);
        a.addEdge("ab", b);
        a.addEdge("ab", b);
        a.addEdge("ab", b);
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);

        this.sqlgGraph.tx().commit();
        a.addEdge("ab", b);
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testMultiplicityWithCountAcrossSchemasAddEdge_1() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("id1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("id2", PropertyDefinition.of(PropertyType.INTEGER));
                }},
                ListOrderedSet.listOrderedSet(Set.of("id1", "id2"))
        );
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("di1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("di2", PropertyDefinition.of(PropertyType.INTEGER));
                }},
                ListOrderedSet.listOrderedSet(Set.of("di1", "di2"))
        );
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(5, 5))
        );
        this.sqlgGraph.tx().commit();

        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A", "id1", 1, "id2", 1);
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", 1, "di2", 1);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", 1, "di2", 2);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", 1, "di2", 3);
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", 1, "di2", 4);
        Vertex b5 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", 1, "di2", 5);
        a.addEdge("ab", b1);
        a.addEdge("ab", b2);
        a.addEdge("ab", b3);
        a.addEdge("ab", b4);
        a.addEdge("ab", b5);
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().commit();
        a.addEdge("ab", b5);
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
            LOGGER.debug("test multiplicity messaging", e);
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
            LOGGER.debug("test multiplicity messaging", e);
        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testMultiplicityWithCountAcrossSchemasAddEdge_2() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("id1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("id2", PropertyDefinition.of(PropertyType.INTEGER));
                }},
                ListOrderedSet.listOrderedSet(Set.of("id1", "id2"))
        );
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(5, 5))
        );
        this.sqlgGraph.tx().commit();

        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A", "id1", 1, "id2", 1);
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", 1, "di2", 1);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", 1, "di2", 2);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", 1, "di2", 3);
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", 1, "di2", 4);
        Vertex b5 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", 1, "di2", 5);
        a.addEdge("ab", b1);
        a.addEdge("ab", b2);
        a.addEdge("ab", b3);
        a.addEdge("ab", b4);
        a.addEdge("ab", b5);
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().commit();
        a.addEdge("ab", b5);
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
            LOGGER.debug("test multiplicity messaging", e);
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
            LOGGER.debug("test multiplicity messaging", e);
        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testMultiplicityWithCountAcrossSchemasAddEdge_3() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("di1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("di2", PropertyDefinition.of(PropertyType.INTEGER));
                }},
                ListOrderedSet.listOrderedSet(Set.of("di1", "di2"))
        );
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(5, 5))
        );
        this.sqlgGraph.tx().commit();

        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A", "id1", 1, "id2", 1);
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", 1, "di2", 1);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", 1, "di2", 2);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", 1, "di2", 3);
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", 1, "di2", 4);
        Vertex b5 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", 1, "di2", 5);
        a.addEdge("ab", b1);
        a.addEdge("ab", b2);
        a.addEdge("ab", b3);
        a.addEdge("ab", b4);
        a.addEdge("ab", b5);
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().commit();
        a.addEdge("ab", b5);
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
            LOGGER.debug("test multiplicity messaging", e);
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
            LOGGER.debug("test multiplicity messaging", e);
        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testMultiplicityWithCountAcrossSchemasRemoveEdge_1() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("di1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("di2", PropertyDefinition.of(PropertyType.INTEGER));
                }},
                ListOrderedSet.listOrderedSet(Set.of("di1", "di2"))
        );
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("id1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("id2", PropertyDefinition.of(PropertyType.INTEGER));
                }},
                ListOrderedSet.listOrderedSet(Set.of("id1", "id2"))
        );
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(4, 5))
        );
        this.sqlgGraph.tx().commit();

        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "id1", 1, "id2", 1);
        try {
            this.sqlgGraph.tx().checkMultiplicity(b1, Direction.IN, edgeLabel, aVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(bVertexLabel, Direction.IN, edgeLabel, aVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }

        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A", "di1", 1, "di2", 1);
        try {
            this.sqlgGraph.tx().checkMultiplicity(b1, Direction.IN, edgeLabel, aVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(bVertexLabel, Direction.IN, edgeLabel, aVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }

        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B.B", "id1", 1, "id2", 2);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B.B", "id1", 1, "id2", 3);
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B.B", "id1", 1, "id2", 4);
        Vertex b5 = this.sqlgGraph.addVertex(T.label, "B.B", "id1", 1, "id2", 5);
        a.addEdge("ab", b1);
        a.addEdge("ab", b2);
        a.addEdge("ab", b3);
        Edge edge4 = a.addEdge("ab", b4);
        Edge edge5 = a.addEdge("ab", b5);
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().commit();

        edge4.remove();
        edge5.remove();
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
    }

    @Test
    public void testMultiplicityWithCountAcrossSchemasRemoveEdge_2() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("di1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("di2", PropertyDefinition.of(PropertyType.INTEGER));
                }},
                ListOrderedSet.listOrderedSet(Set.of("di1", "di2"))
        );
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(4, 5))
        );
        this.sqlgGraph.tx().commit();

        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "id1", 1, "id2", 1);
        try {
            this.sqlgGraph.tx().checkMultiplicity(b1, Direction.IN, edgeLabel, aVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(bVertexLabel, Direction.IN, edgeLabel, aVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }

        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A", "di1", 1, "di2", 1);
        try {
            this.sqlgGraph.tx().checkMultiplicity(b1, Direction.IN, edgeLabel, aVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(bVertexLabel, Direction.IN, edgeLabel, aVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }

        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B.B", "id1", 1, "id2", 2);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B.B", "id1", 1, "id2", 3);
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B.B", "id1", 1, "id2", 4);
        Vertex b5 = this.sqlgGraph.addVertex(T.label, "B.B", "id1", 1, "id2", 5);
        a.addEdge("ab", b1);
        a.addEdge("ab", b2);
        a.addEdge("ab", b3);
        Edge edge4 = a.addEdge("ab", b4);
        Edge edge5 = a.addEdge("ab", b5);
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().commit();

        edge4.remove();
        edge5.remove();
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
    }

    @Test
    public void testMultiplicityWithCountAcrossSchemasRemoveEdge_3() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("id1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("id2", PropertyDefinition.of(PropertyType.INTEGER));
                }},
                ListOrderedSet.listOrderedSet(Set.of("id1", "id2"))
        );
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(4, 5))
        );
        this.sqlgGraph.tx().commit();

        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "id1", 1, "id2", 1);
        try {
            this.sqlgGraph.tx().checkMultiplicity(b1, Direction.IN, edgeLabel, aVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(bVertexLabel, Direction.IN, edgeLabel, aVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }

        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A", "di1", 1, "di2", 1);
        try {
            this.sqlgGraph.tx().checkMultiplicity(b1, Direction.IN, edgeLabel, aVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(bVertexLabel, Direction.IN, edgeLabel, aVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }

        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B.B", "id1", 1, "id2", 2);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B.B", "id1", 1, "id2", 3);
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B.B", "id1", 1, "id2", 4);
        Vertex b5 = this.sqlgGraph.addVertex(T.label, "B.B", "id1", 1, "id2", 5);
        a.addEdge("ab", b1);
        a.addEdge("ab", b2);
        a.addEdge("ab", b3);
        Edge edge4 = a.addEdge("ab", b4);
        Edge edge5 = a.addEdge("ab", b5);
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().commit();

        edge4.remove();
        edge5.remove();
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
    }

    @Test
    public void testMultiplicityWithCountAcrossSchemasDropEdges_1() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("id1", PropertyDefinition.of(PropertyType.STRING));
                    put("id2", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(List.of("id1", "id2"))
        );
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("di1", PropertyDefinition.of(PropertyType.STRING));
                    put("di2", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(List.of("di1", "di2"))
        );
        VertexLabel cVertexLabel = bSchema.ensureVertexLabelExist("C",
                new HashMap<>() {{
                    put("i1", PropertyDefinition.of(PropertyType.STRING));
                    put("i2", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(List.of("i1", "i2"))
        );
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(0, 1),
                        Multiplicity.of(4, 5))
        );
        cVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(0, -1),
                        Multiplicity.of(0, -1))
        );
        this.sqlgGraph.tx().commit();

        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A", "id1", "1", "id2", "1");
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", "1", "di2", "1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", "1", "di2", "2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", "1", "di2", "3");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", "1", "di2", "4");
        Vertex b5 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", "1", "di2", "5");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "B.C", "i1", "1", "i2", "1");
        Edge edge1 = a.addEdge("ab", b1);
        Edge edge2 = a.addEdge("ab", b2);
        Edge edge3 = a.addEdge("ab", b3);
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        a.addEdge("ab", b4);
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
        a.addEdge("ab", b5);
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
        c1.addEdge("ab", b5);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.traversal().V().hasLabel("A.A").out().toList();

        this.sqlgGraph.traversal().V().hasLabel("A.A").outE().hasId(P.within(edge1.id(), edge2.id(), edge3.id())).drop().iterate();
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
    }

    @Test
    public void testMultiplicityWithCountAcrossSchemasDropEdges_2() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("di1", PropertyDefinition.of(PropertyType.STRING));
                    put("di2", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(List.of("di1", "di2"))
        );
        VertexLabel cVertexLabel = bSchema.ensureVertexLabelExist("C",
                new HashMap<>() {{
                    put("i1", PropertyDefinition.of(PropertyType.STRING));
                    put("i2", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(List.of("i1", "i2"))
        );
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(0, 1),
                        Multiplicity.of(4, 5))
        );
        cVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(0, -1),
                        Multiplicity.of(0, -1))
        );
        this.sqlgGraph.tx().commit();

        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A", "id1", "1", "id2", "1");
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", "1", "di2", "1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", "1", "di2", "2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", "1", "di2", "3");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", "1", "di2", "4");
        Vertex b5 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", "1", "di2", "5");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "B.C", "i1", "1", "i2", "1");
        Edge edge1 = a.addEdge("ab", b1);
        Edge edge2 = a.addEdge("ab", b2);
        Edge edge3 = a.addEdge("ab", b3);
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        a.addEdge("ab", b4);
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
        a.addEdge("ab", b5);
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
        c1.addEdge("ab", b5);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.traversal().V().hasLabel("A.A").out().toList();

        this.sqlgGraph.traversal().V().hasLabel("A.A").outE().hasId(P.within(edge1.id(), edge2.id(), edge3.id())).drop().iterate();
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
    }

    @Test
    public void testMultiplicityWithCountAcrossSchemasDropEdges_3() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("id1", PropertyDefinition.of(PropertyType.STRING));
                    put("id2", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(List.of("id1", "id2"))
        );
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B");
        VertexLabel cVertexLabel = bSchema.ensureVertexLabelExist("C",
                new HashMap<>() {{
                    put("i1", PropertyDefinition.of(PropertyType.STRING));
                    put("i2", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(List.of("i1", "i2"))
        );
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(0, 1),
                        Multiplicity.of(4, 5))
        );
        cVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(0, -1),
                        Multiplicity.of(0, -1))
        );
        this.sqlgGraph.tx().commit();

        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A", "id1", "1", "id2", "1");
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", "1", "di2", "1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", "1", "di2", "2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", "1", "di2", "3");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", "1", "di2", "4");
        Vertex b5 = this.sqlgGraph.addVertex(T.label, "B.B", "di1", "1", "di2", "5");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "B.C", "i1", "1", "i2", "1");
        Edge edge1 = a.addEdge("ab", b1);
        Edge edge2 = a.addEdge("ab", b2);
        Edge edge3 = a.addEdge("ab", b3);
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        a.addEdge("ab", b4);
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
        a.addEdge("ab", b5);
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
        c1.addEdge("ab", b5);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.traversal().V().hasLabel("A.A").out().toList();

        this.sqlgGraph.traversal().V().hasLabel("A.A").outE().hasId(P.within(edge1.id(), edge2.id(), edge3.id())).drop().iterate();
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
    }

    @Test
    public void testEdges_1() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("a", PropertyDefinition.of(PropertyType.STRING));
                    put("b", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(List.of("a", "b"))
        );
        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("c", PropertyDefinition.of(PropertyType.STRING));
                    put("d", PropertyDefinition.of(PropertyType.STRING));

                }},
                ListOrderedSet.listOrderedSet(List.of("c", "d"))
        );
        VertexLabel cVertexLabel = publicSchema.ensureVertexLabelExist("C");
        EdgeLabel ab = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(2, 2)));
        bVertexLabel.ensureEdgeLabelExist("ab", cVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(3, 3),
                        Multiplicity.of(4, 4)));
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.getTopology().lock();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "a", "a", "b", "b");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "c", "c", "d", "d");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        a.addEdge("ab", b);
        a.addEdge("ab", c);
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, ab, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, ab, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testEdges_2() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("c", PropertyDefinition.of(PropertyType.STRING));
                    put("d", PropertyDefinition.of(PropertyType.STRING));

                }},
                ListOrderedSet.listOrderedSet(List.of("c", "d"))
        );
        VertexLabel cVertexLabel = publicSchema.ensureVertexLabelExist("C");
        EdgeLabel ab = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(2, 2)));
        bVertexLabel.ensureEdgeLabelExist("ab", cVertexLabel,
                new EdgeDefinition(
                        Multiplicity.of(3, 3),
                        Multiplicity.of(4, 4)));
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.getTopology().lock();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "c", "c", "d", "d");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        a.addEdge("ab", b);
        a.addEdge("ab", c);
        try {
            this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, ab, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, ab, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testCheckMultiplicityForAllEdges() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");

        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A", new HashMap<>() {{
                    put("a", PropertyDefinition.of(PropertyType.UUID));
                    put("b", PropertyDefinition.of(PropertyType.UUID));
                }},
                ListOrderedSet.listOrderedSet(List.of("a", "b"))
        );
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("c", PropertyDefinition.of(PropertyType.UUID));
                    put("d", PropertyDefinition.of(PropertyType.UUID));
                }},
                ListOrderedSet.listOrderedSet(List.of("c", "d"))
        );
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new EdgeDefinition(Multiplicity.of(1, 1), Multiplicity.of(1, 5))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A", "a", UUID.randomUUID(), "b", UUID.randomUUID());
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "c", UUID.randomUUID(), "d", UUID.randomUUID());
        a1.addEdge("ab", b1);

        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A.A", "a", UUID.randomUUID(), "b", UUID.randomUUID());

        try {
            this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        a2.addEdge("ab", b1);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
        try {
            this.sqlgGraph.tx().checkMultiplicity(bVertexLabel, Direction.IN, edgeLabel, aVertexLabel);
            Assert.fail("Expected multiplicity failure.");
        } catch (IllegalStateException e) {
            //noop
        }
        this.sqlgGraph.tx().commit();

    }
}
