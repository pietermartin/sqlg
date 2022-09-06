package org.umlg.sqlg.test.topology.edgeMultiplicity;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.topology.EdgeDefinition;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

@SuppressWarnings("DuplicatedCode")
public class TestMultiplicityAddRemoveEdge extends BaseTest {

    @Test
    public void testMultiplicityWithCount() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(1, 5))
        );
        this.sqlgGraph.tx().commit();

        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
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
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
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
    public void testMultiplicityWithCountAcrossSchemasAddEdge() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(5, 5))
        );
        this.sqlgGraph.tx().commit();

        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B.B");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B.B");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B.B");
        Vertex b5 = this.sqlgGraph.addVertex(T.label, "B.B");
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
    public void testMultiplicityWithCountAcrossSchemasRemoveEdge() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(4, 5))
        );
        this.sqlgGraph.tx().commit();

        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B");
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

        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A");
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

        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B.B");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B.B");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B.B");
        Vertex b5 = this.sqlgGraph.addVertex(T.label, "B.B");
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
    public void testMultiplicityWithCountAcrossSchemasDropEdges() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B");
        VertexLabel cVertexLabel = bSchema.ensureVertexLabelExist("C");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, 1),
                        Multiplicity.of(4, 5))
        );
        cVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1),
                        Multiplicity.of(0, -1))
        );
        this.sqlgGraph.tx().commit();

        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A");
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
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B.B");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B.B");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B.B");
        Vertex b5 = this.sqlgGraph.addVertex(T.label, "B.B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "B.C");
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
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, edgeLabel, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, edgeLabel, bVertexLabel);
    }

    @Test
    public void testEdges() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B");
        VertexLabel cVertexLabel = publicSchema.ensureVertexLabelExist("C");
        EdgeLabel ab = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(1, 1),
                        Multiplicity.of(2, 2)));
        bVertexLabel.ensureEdgeLabelExist("ab", cVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(3, 3),
                        Multiplicity.of(4, 4)));
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.getTopology().lock();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
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
        a = this.sqlgGraph.addVertex(T.label, "A");
        b = this.sqlgGraph.addVertex(T.label, "B");
        a.addEdge("ab", b);
        a.addEdge("ab", b);
        this.sqlgGraph.tx().checkMultiplicity(a, Direction.OUT, ab, bVertexLabel);
        this.sqlgGraph.tx().checkMultiplicity(aVertexLabel, Direction.OUT, ab, bVertexLabel);
    }

    @Test
    public void testCheckMultiplicityForAllEdges() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");

        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(Multiplicity.of(1, 1), Multiplicity.of(1, 5))
        );
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B");
        a1.addEdge("ab", b1);

        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A.A");

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
