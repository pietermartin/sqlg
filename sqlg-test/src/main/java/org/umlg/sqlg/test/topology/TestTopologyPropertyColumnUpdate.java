package org.umlg.sqlg.test.topology;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

public class TestTopologyPropertyColumnUpdate extends BaseTest {

    private final List<Triple<TopologyInf, TopologyInf, TopologyChangeAction>> topologyListenerTriple = new ArrayList<>();

    @Before
    public void before() throws Exception {
        Assume.assumeTrue(!isMariaDb());
        super.before();
        this.topologyListenerTriple.clear();
    }

    @Test
    public void testPropertyUpdateMultiplicityFrom1to0() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                    put("column1", PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1)));
                }});
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.addVertex(T.label, "A");
            Assert.fail("NOT NULL should have prevented this");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }


        Optional<VertexLabel> aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        Preconditions.checkState(aVertexLabelOptional.isPresent());
        VertexLabel aVertexLabel = aVertexLabelOptional.get();
        Optional<PropertyColumn> column1Optional = aVertexLabel.getProperty("column1");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //Change the property from required to optional
        this.topologyListenerTriple.clear();
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1)));
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.addVertex(T.label, "A");
        } catch (Exception e) {
            Assert.fail("NOT NULL should have been dropped");
            this.sqlgGraph.tx().rollback();
        }
        this.sqlgGraph.tx().rollback();

        Assert.assertEquals(1, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(0).getRight());
        column1Optional = aVertexLabel.getProperty("column1");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(0, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1)));
        List<Long> lowers = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<Long>values(Topology.SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER).toList();
        Assert.assertEquals(1, lowers.size());
        Assert.assertEquals(1L, lowers.get(0).longValue());
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            lowers = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<Long>values(Topology.SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER).toList();
            Assert.assertEquals(1, lowers.size());
            Assert.assertEquals(0L, lowers.get(0).longValue());
        }
    }

    @Test
    public void testPropertyUpdateMultiplicityFrom1to0_OnEdgeLabel() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(),
                new LinkedHashMap<>() {{
                    put("column1", PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1)));
                }}
        );
        this.sqlgGraph.tx().commit();
        try {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b);
            Assert.fail("NOT NULL should have prevented this");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }


        Optional<PropertyColumn> column1Optional = edgeLabel.getProperty("column1");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //Change the property from required to optional
        this.topologyListenerTriple.clear();
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1)));
        this.sqlgGraph.tx().commit();
        try {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b);
        } catch (Exception e) {
            Assert.fail("NOT NULL should have been dropped");
            this.sqlgGraph.tx().rollback();
        }
        this.sqlgGraph.tx().rollback();

        Assert.assertEquals(1, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(0).getRight());
        column1Optional = edgeLabel.getProperty("column1");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(0, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1)));
        List<Long> lowers = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<Long>values(Topology.SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER).toList();
        Assert.assertEquals(1, lowers.size());
        Assert.assertEquals(1L, lowers.get(0).longValue());
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            lowers = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<Long>values(Topology.SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER).toList();
            Assert.assertEquals(1, lowers.size());
            Assert.assertEquals(0L, lowers.get(0).longValue());
        }
    }

    @Test
    public void testPropertyUpdateMultiplicityFrom0to1() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                    put("column1", PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1)));
                }});
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().rollback();

        Optional<VertexLabel> aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        Preconditions.checkState(aVertexLabelOptional.isPresent());
        VertexLabel aVertexLabel = aVertexLabelOptional.get();
        Optional<PropertyColumn> column1Optional = aVertexLabel.getProperty("column1");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //Change the property from required to optional
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1)));
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.addVertex(T.label, "A");
            Assert.fail("NOT NULL should have been dropped");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }
        this.sqlgGraph.tx().rollback();

        Assert.assertEquals(2, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(1).getRight());
        column1Optional = aVertexLabel.getProperty("column1");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1)));
        List<Long> lowers = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<Long>values(Topology.SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER).toList();
        Assert.assertEquals(1, lowers.size());
        Assert.assertEquals(0L, lowers.get(0).longValue());
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            lowers = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<Long>values(Topology.SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER).toList();
            Assert.assertEquals(1, lowers.size());
            Assert.assertEquals(1L, lowers.get(0).longValue());
        }
    }

    @Test
    public void testPropertyUpdateMultiplicityFrom0to1_OnEdgeLabel() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(),
                new LinkedHashMap<>() {{
                    put("column1", PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1)));
                }}
        );
        this.sqlgGraph.tx().commit();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        a.addEdge("ab", b);
        this.sqlgGraph.tx().rollback();

        Optional<PropertyColumn> column1Optional = edgeLabel.getProperty("column1");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //Change the property from required to optional
        this.topologyListenerTriple.clear();
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1)));
        this.sqlgGraph.tx().commit();
        try {
            a = this.sqlgGraph.addVertex(T.label, "A");
            b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b);
            Assert.fail("NOT NULL should have been dropped");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }
        this.sqlgGraph.tx().rollback();

        Assert.assertEquals(1, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(0).getRight());
        column1Optional = edgeLabel.getProperty("column1");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1)));
        List<Long> lowers = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<Long>values(Topology.SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER).toList();
        Assert.assertEquals(1, lowers.size());
        Assert.assertEquals(0L, lowers.get(0).longValue());
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            lowers = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<Long>values(Topology.SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER).toList();
            Assert.assertEquals(1, lowers.size());
            Assert.assertEquals(1L, lowers.get(0).longValue());
        }
    }

    @Test
    public void testPropertyUpdateDropCheckConstraint() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')"));
                }});
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.addVertex(T.label, "A", "name", "a");
            Assert.fail("check constraint should have prevented this");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }

        Optional<VertexLabel> aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        Preconditions.checkState(aVertexLabelOptional.isPresent());
        VertexLabel aVertexLabel = aVertexLabelOptional.get();
        Optional<PropertyColumn> column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //drop the check constraint
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1)));
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        this.sqlgGraph.tx().commit();
        v.remove();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(1).getRight());
        column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(0, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')"));
        List<String> checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
        Assert.assertEquals(1, checkConstraints.size());
        Assert.assertEquals("(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')", checkConstraints.get(0));
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
            Assert.assertEquals(0, checkConstraints.size());
        }
    }

    @Test
    public void testPropertyUpdateDropCheckConstraint_OnEdge() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, EdgeDefinition.of(),
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')"));
                }}
        );
        this.sqlgGraph.tx().commit();
        try {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b, "name", "a");
            Assert.fail("check constraint should have prevented this");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }

        Optional<PropertyColumn> column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //drop the check constraint
        this.topologyListenerTriple.clear();
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1)));
        this.sqlgGraph.tx().commit();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Edge e = a.addEdge("ab", b, "name", "a");
        this.sqlgGraph.tx().commit();
        e.remove();
        a.remove();
        b.remove();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(0).getRight());
        column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(0, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')"));
        List<String> checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
        Assert.assertEquals(1, checkConstraints.size());
        Assert.assertEquals("(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')", checkConstraints.get(0));
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
            Assert.assertEquals(0, checkConstraints.size());
        }
    }

    @Test
    public void testPropertyUpdateAddCheckConstraint() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1)));
                }});
        this.sqlgGraph.tx().commit();

        Optional<VertexLabel> aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        Preconditions.checkState(aVertexLabelOptional.isPresent());
        VertexLabel aVertexLabel = aVertexLabelOptional.get();
        Optional<PropertyColumn> column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')"));
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.addVertex(T.label, "A", "name", "a");
            Assert.fail("check constraint should have prevented this!!!");
        } catch (Exception e) {
            this.sqlgGraph.tx().commit();
        }

        Assert.assertEquals(2, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(1).getRight());
        column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(0, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1)));
        List<String> checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
        Assert.assertEquals(0, checkConstraints.size());
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
            Assert.assertEquals(1, checkConstraints.size());
            Assert.assertEquals("(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')", checkConstraints.get(0));
        }
    }

    @Test
    public void testPropertyUpdateAddCheckConstraint_OnEdgeLabel() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(),
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1)));
                }}
        );
        this.sqlgGraph.tx().commit();

        Optional<PropertyColumn> column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        this.topologyListenerTriple.clear();
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')"));
        this.sqlgGraph.tx().commit();
        try {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b, "name", "a");
            Assert.fail("check constraint should have prevented this!!!");
        } catch (Exception e) {
            this.sqlgGraph.tx().commit();
        }

        Assert.assertEquals(1, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(0).getRight());
        column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(0, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1)));
        List<String> checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
        Assert.assertEquals(0, checkConstraints.size());
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
            Assert.assertEquals(1, checkConstraints.size());
            Assert.assertEquals("(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')", checkConstraints.get(0));
        }
    }

    @Test
    public void testPropertyUpdateAddCheckConstraintWithFailure() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                    put(
                            "name",
                            PropertyDefinition.of(
                                    PropertyType.varChar(10),
                                    Multiplicity.of(0, 1),
                                    null,
                                    "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')"
                            )
                    );
                }});
        this.sqlgGraph.tx().commit();

        Optional<VertexLabel> aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        Preconditions.checkState(aVertexLabelOptional.isPresent());
        VertexLabel aVertexLabel = aVertexLabelOptional.get();
        Optional<PropertyColumn> column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        try {
            column1.updatePropertyDefinition(
                    PropertyDefinition.of(
                            PropertyType.varChar(10),
                            Multiplicity.of(0, 1),
                            null,
                            "fail"
                    )
            );
            Assert.fail();
            this.sqlgGraph.tx().commit();
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }
        column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(0, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());
        String checkConstraint = column1.getPropertyDefinition().checkConstraint();
        Assert.assertEquals("(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')", checkConstraint);

        //do it again, testing bug in not clearing updateProperties in rollback
        try {
            column1.updatePropertyDefinition(
                    PropertyDefinition.of(
                            PropertyType.varChar(10),
                            Multiplicity.of(0, 1),
                            null,
                            "fail"
                    )
            );
            Assert.fail("Code should not make it to here, expected db constraint failure");
            this.sqlgGraph.tx().commit();
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            List<String> checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
            Assert.assertEquals(1, checkConstraints.size());
            Assert.assertEquals("(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')", checkConstraints.get(0));
        }
    }

    @Test
    public void testPropertyUpdateChangeCheckConstraint() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')"));
                }});
        this.sqlgGraph.tx().commit();

        try {
            this.sqlgGraph.addVertex(T.label, "A", "name", "a");
            Assert.fail("check constraint should have prevented this.");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }

        Optional<VertexLabel> aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        Preconditions.checkState(aVertexLabelOptional.isPresent());
        VertexLabel aVertexLabel = aVertexLabelOptional.get();
        Optional<PropertyColumn> column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'b')"));
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        this.sqlgGraph.tx().commit();
        v.remove();
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.addVertex(T.label, "A", "name", "b");
            Assert.fail("check constraint should have prevented this!!!");
        } catch (Exception e) {
            this.sqlgGraph.tx().commit();
        }

        Assert.assertEquals(2, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(1).getRight());
        column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(0, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')"));

        List<String> checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
        Assert.assertEquals(1, checkConstraints.size());
        Assert.assertEquals("(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')", checkConstraints.get(0));
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
            Assert.assertEquals(1, checkConstraints.size());
            Assert.assertEquals("(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'b')", checkConstraints.get(0));
        }
    }

    @Test
    public void testPropertyUpdateChangeCheckConstraint_OnEdgeLabel() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(),
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')"));
                }}
        );
        this.sqlgGraph.tx().commit();

        try {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b, "name", "a");
            Assert.fail("check constraint should have prevented this.");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }

        Optional<PropertyColumn> column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        this.topologyListenerTriple.clear();
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(0, 1), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'b')"));
        this.sqlgGraph.tx().commit();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Edge edge = a.addEdge("ab", b, "name", "a");
        this.sqlgGraph.tx().commit();
        edge.remove();
        a.remove();
        b.remove();
        this.sqlgGraph.tx().commit();
        try {
            a = this.sqlgGraph.addVertex(T.label, "A");
            b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b, "name", "b");
            Assert.fail("check constraint should have prevented this!!!");
        } catch (Exception e) {
            this.sqlgGraph.tx().commit();
        }

        Assert.assertEquals(1, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(0).getRight());
        column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(0, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')"));

        List<String> checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
        Assert.assertEquals(1, checkConstraints.size());
        Assert.assertEquals("(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'a')", checkConstraints.get(0));
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
            Assert.assertEquals(1, checkConstraints.size());
            Assert.assertEquals("(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " <> 'b')", checkConstraints.get(0));
        }
    }

    @Test
    public void testPropertyUpdateArrayMultiplicityAndCheckConstraintH2() {
        Assume.assumeTrue(isH2());
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING_ARRAY, Multiplicity.of(2, 4), null,
                            "(ARRAY_CONTAINS (" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + ", 'a1'))")
                    );
                }});
        this.sqlgGraph.tx().commit();

        try {
            this.sqlgGraph.addVertex(T.label, "A", "name", new String[]{"b1", "b2"});
            Assert.fail("check constraint should have prevented this.");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }

        Optional<VertexLabel> aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        Preconditions.checkState(aVertexLabelOptional.isPresent());
        VertexLabel aVertexLabel = aVertexLabelOptional.get();
        Optional<PropertyColumn> column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.STRING_ARRAY, Multiplicity.of(2, 4), null,
                "(ARRAY_CONTAINS (" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + ", 'b1'))")
        );
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.addVertex(T.label, "A", "name", new String[]{"b1", "b2"});
        this.sqlgGraph.tx().commit();
        v.remove();
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.addVertex(T.label, "A", "name", new String[]{"a1"});
            Assert.fail("check constraint should have prevented this!!!");
        } catch (Exception e) {
            this.sqlgGraph.tx().commit();
        }

        Assert.assertEquals(2, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(1).getRight());
        column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(2, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(4, column1.getPropertyDefinition().multiplicity().upper());
    }

    @Test
    public void testPropertyUpdateArrayMultiplicityAndCheckConstraintH2_OnEdgeLabel() {
        Assume.assumeTrue(isH2());
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(),
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING_ARRAY, Multiplicity.of(2, 4), null,
                            "(ARRAY_CONTAINS (" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + ", 'a1'))")
                    );
                }}
        );
        this.sqlgGraph.tx().commit();

        try {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b, "name", new String[]{"b1", "b2"});
            Assert.fail("check constraint should have prevented this.");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }

        Optional<PropertyColumn> column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        this.topologyListenerTriple.clear();
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.STRING_ARRAY, Multiplicity.of(2, 4), null,
                "(ARRAY_CONTAINS (" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + ", 'b1'))")
        );
        this.sqlgGraph.tx().commit();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Edge edge = a.addEdge("ab", b, "name", new String[]{"b1", "b2"});
        this.sqlgGraph.tx().commit();
        edge.remove();
        a.remove();
        b.remove();
        this.sqlgGraph.tx().commit();
        try {
            a = this.sqlgGraph.addVertex(T.label, "A");
            b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b, "name", new String[]{"a1"});
            Assert.fail("check constraint should have prevented this!!!");
        } catch (Exception e) {
            this.sqlgGraph.tx().commit();
        }

        Assert.assertEquals(1, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(0).getRight());
        column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(2, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(4, column1.getPropertyDefinition().multiplicity().upper());
    }

    @Test
    public void testPropertyUpdateArrayMultiplicityAndCheckConstraintHsqldb() {
        Assume.assumeTrue(isHsqldb());
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING_ARRAY, Multiplicity.of(2, 4), null,
                            "(POSITION_ARRAY ('{a1}' IN " + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + ") = -1)")
                    );
                }});
        this.sqlgGraph.tx().commit();

        try {
            this.sqlgGraph.addVertex(T.label, "A", "name", new String[]{"b1", "b2"});
            Assert.fail("check constraint should have prevented this.");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }

        Optional<VertexLabel> aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        Preconditions.checkState(aVertexLabelOptional.isPresent());
        VertexLabel aVertexLabel = aVertexLabelOptional.get();
        Optional<PropertyColumn> column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.STRING_ARRAY, Multiplicity.of(2, 4), null,
                "(POSITION_ARRAY ('{b1}' IN " + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + ") != -1)")
        );
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.addVertex(T.label, "A", "name", new String[]{"b1", "b2"});
        this.sqlgGraph.tx().commit();
        v.remove();
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.addVertex(T.label, "A", "name", new String[]{"a1"});
            Assert.fail("check constraint should have prevented this!!!");
        } catch (Exception e) {
            this.sqlgGraph.tx().commit();
        }

        Assert.assertEquals(2, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(1).getRight());
        column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(2, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(4, column1.getPropertyDefinition().multiplicity().upper());
    }

    @Test
    public void testPropertyUpdateArrayMultiplicityAndCheckConstraintHsqldb_OnEdgeLabel() {
        Assume.assumeTrue(isHsqldb());
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(),
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING_ARRAY, Multiplicity.of(2, 4), null,
                            "(POSITION_ARRAY ('{a1}' IN " + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + ") = -1)")
                    );
                }}
        );
        this.sqlgGraph.tx().commit();

        try {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b, "name", new String[]{"b1", "b2"});
            Assert.fail("check constraint should have prevented this.");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }

        Optional<PropertyColumn> column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        this.topologyListenerTriple.clear();
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.STRING_ARRAY, Multiplicity.of(2, 4), null,
                "(POSITION_ARRAY ('{b1}' IN " + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + ") != -1)")
        );
        this.sqlgGraph.tx().commit();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Edge edge = a.addEdge("ab", b, "name", new String[]{"b1", "b2"});
        this.sqlgGraph.tx().commit();
        edge.remove();
        a.remove();
        b.remove();
        this.sqlgGraph.tx().commit();
        try {
            a = this.sqlgGraph.addVertex(T.label, "A");
            b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b, "name", new String[]{"a1"});
            Assert.fail("check constraint should have prevented this!!!");
        } catch (Exception e) {
            this.sqlgGraph.tx().commit();
        }

        Assert.assertEquals(1, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(0).getRight());
        column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(2, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(4, column1.getPropertyDefinition().multiplicity().upper());
    }

    @Test
    public void testPropertyUpdateArrayMultiplicityAndCheckConstraint() {
        Assume.assumeTrue(isPostgres());
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING_ARRAY, Multiplicity.of(2, 4), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " @> '{a1}')"));
                }});
        this.sqlgGraph.tx().commit();

        try {
            this.sqlgGraph.addVertex(T.label, "A", "name", new String[]{"b1", "b2"});
            Assert.fail("check constraint should have prevented this.");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }

        Optional<VertexLabel> aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        Preconditions.checkState(aVertexLabelOptional.isPresent());
        VertexLabel aVertexLabel = aVertexLabelOptional.get();
        Optional<PropertyColumn> column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.STRING_ARRAY, Multiplicity.of(2, 4), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " @> '{b2}')"));
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.addVertex(T.label, "A", "name", new String[]{"b1", "b2"});
        this.sqlgGraph.tx().commit();
        v.remove();
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.addVertex(T.label, "A", "name", new String[]{"a1"});
            Assert.fail("check constraint should have prevented this!!!");
        } catch (Exception e) {
            this.sqlgGraph.tx().commit();
        }

        Assert.assertEquals(2, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(1).getRight());
        column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(2, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(4, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.STRING_ARRAY, Multiplicity.of(2, 4), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " @> '{a1}')"));
        List<String> checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
        Assert.assertEquals(1, checkConstraints.size());
        Assert.assertEquals("(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " @> '{a1}')", checkConstraints.get(0));
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
            Assert.assertEquals(1, checkConstraints.size());
            Assert.assertEquals("(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " @> '{b2}')", checkConstraints.get(0));
        }
    }

    @Test
    public void testPropertyUpdateArrayMultiplicityAndCheckConstraint_OnEdgeLabel() {
        Assume.assumeTrue(isPostgres());
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(),
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING_ARRAY, Multiplicity.of(2, 4), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " @> '{a1}')"));
                }}
        );
        this.sqlgGraph.tx().commit();
        try {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b, "name", new String[]{"b1", "b2"});
            Assert.fail("check constraint should have prevented this.");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }

        Optional<PropertyColumn> column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        this.topologyListenerTriple.clear();
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.STRING_ARRAY, Multiplicity.of(2, 4), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " @> '{b2}')"));
        this.sqlgGraph.tx().commit();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Edge edge = a.addEdge("ab", b, "name", new String[]{"b1", "b2"});
        this.sqlgGraph.tx().commit();
        edge.remove();
        a.remove();
        b.remove();
        this.sqlgGraph.tx().commit();
        try {
            a = this.sqlgGraph.addVertex(T.label, "A");
            b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b, "name", new String[]{"a1"});
            Assert.fail("check constraint should have prevented this!!!");
        } catch (Exception e) {
            this.sqlgGraph.tx().commit();
        }

        Assert.assertEquals(1, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(0).getRight());
        column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(2, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(4, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.STRING_ARRAY, Multiplicity.of(2, 4), null, "(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " @> '{a1}')"));
        List<String> checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
        Assert.assertEquals(1, checkConstraints.size());
        Assert.assertEquals("(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " @> '{a1}')", checkConstraints.get(0));
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            checkConstraints = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT).toList();
            Assert.assertEquals(1, checkConstraints.size());
            Assert.assertEquals("(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("name") + " @> '{b2}')", checkConstraints.get(0));
        }
    }

    @Test
    public void testPropertyUpdateDefaultLiteral() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1), "'a'"));
                }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        String name = this.sqlgGraph.traversal().V().hasLabel("A").tryNext().orElseThrow().value("name");
        Assert.assertEquals("a", name);
        this.sqlgGraph.traversal().V().drop().iterate();
        this.sqlgGraph.tx().commit();

        Optional<VertexLabel> aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        Preconditions.checkState(aVertexLabelOptional.isPresent());
        VertexLabel aVertexLabel = aVertexLabelOptional.get();
        Optional<PropertyColumn> column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1), "'b'"));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        name = this.sqlgGraph.traversal().V().hasLabel("A").tryNext().orElseThrow().value("name");
        Assert.assertEquals("b", name);

        Assert.assertEquals(2, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(1).getRight());
        column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1), "'a'"));
        List<String> defaultLiterals = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL).toList();
        Assert.assertEquals(1, defaultLiterals.size());
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            defaultLiterals = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL).toList();
            Assert.assertEquals(1, defaultLiterals.size());
            Assert.assertEquals("'b'", defaultLiterals.get(0));
        }
    }

    @Test
    public void testPropertyUpdateDefaultLiteral_OnEdgeLabel() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(),
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1), "'a'"));
                }}
        );
        this.sqlgGraph.tx().commit();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        a.addEdge("ab", b);
        this.sqlgGraph.tx().commit();
        String name = this.sqlgGraph.traversal().E().hasLabel("ab").tryNext().orElseThrow().value("name");
        Assert.assertEquals("a", name);
        this.sqlgGraph.traversal().V().drop().iterate();
        this.sqlgGraph.tx().commit();

        Optional<PropertyColumn> column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        this.topologyListenerTriple.clear();
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1), "'b'"));
        this.sqlgGraph.tx().commit();
        a = this.sqlgGraph.addVertex(T.label, "A");
        b = this.sqlgGraph.addVertex(T.label, "B");
        a.addEdge("ab", b);
        this.sqlgGraph.tx().commit();
        name = this.sqlgGraph.traversal().E().hasLabel("ab").tryNext().orElseThrow().value("name");
        Assert.assertEquals("b", name);

        Assert.assertEquals(1, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(0).getRight());
        column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1), "'a'"));
        List<String> defaultLiterals = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL).toList();
        Assert.assertEquals(1, defaultLiterals.size());
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            defaultLiterals = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL).toList();
            Assert.assertEquals(1, defaultLiterals.size());
            Assert.assertEquals("'b'", defaultLiterals.get(0));
        }
    }

    @Test
    public void testPropertyRemoveDefaultLiteral() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1), "'a'"));
                }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        String name = this.sqlgGraph.traversal().V().hasLabel("A").tryNext().orElseThrow().value("name");
        Assert.assertEquals("a", name);
        this.sqlgGraph.traversal().V().drop().iterate();
        this.sqlgGraph.tx().commit();

        Optional<VertexLabel> aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        Preconditions.checkState(aVertexLabelOptional.isPresent());
        VertexLabel aVertexLabel = aVertexLabelOptional.get();
        Optional<PropertyColumn> column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1)));
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.addVertex(T.label, "A");
            Assert.fail("required column should have prevented this.");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();

        }

        Assert.assertEquals(2, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(1).getRight());
        column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1), "'a'"));
        List<String> defaultLiterals = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL).toList();
        Assert.assertEquals(1, defaultLiterals.size());
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            defaultLiterals = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL).toList();
            Assert.assertEquals(0, defaultLiterals.size());
        }
    }

    @Test
    public void testPropertyRemoveDefaultLiteral_OnEdgeLabel() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(),
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1), "'a'"));
                }}
        );
        this.sqlgGraph.tx().commit();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Edge edge = a.addEdge("ab", b);
        this.sqlgGraph.tx().commit();
        String name = this.sqlgGraph.traversal().E().hasLabel("ab").tryNext().orElseThrow().value("name");
        Assert.assertEquals("a", name);
        this.sqlgGraph.traversal().V().drop().iterate();
        this.sqlgGraph.tx().commit();

        Optional<PropertyColumn> column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        this.topologyListenerTriple.clear();
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1)));
        this.sqlgGraph.tx().commit();
        try {
            a = this.sqlgGraph.addVertex(T.label, "A");
            b = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b);
            Assert.fail("required column should have prevented this.");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }

        Assert.assertEquals(1, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(0).getRight());
        column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1), "'a'"));
        List<String> defaultLiterals = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL).toList();
        Assert.assertEquals(1, defaultLiterals.size());
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            defaultLiterals = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL).toList();
            Assert.assertEquals(0, defaultLiterals.size());
        }
    }

    @Test
    public void testPropertyAddDefaultLiteral() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1)));
                }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        this.sqlgGraph.tx().commit();

        Optional<VertexLabel> aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        Preconditions.checkState(aVertexLabelOptional.isPresent());
        VertexLabel aVertexLabel = aVertexLabelOptional.get();
        Optional<PropertyColumn> column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1), "'b'"));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();

        List<Vertex> b = this.sqlgGraph.traversal().V().hasLabel("A").has("name", "b").toList();
        Assert.assertEquals(1, b.size());

        Assert.assertEquals(2, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(1).getRight());
        column1Optional = aVertexLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1)));
        List<String> defaultLiterals = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL).toList();
        Assert.assertEquals(0, defaultLiterals.size());
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            defaultLiterals = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL).toList();
            Assert.assertEquals(1, defaultLiterals.size());
            Assert.assertEquals("'b'", defaultLiterals.get(0));
        }
    }

    @Test
    public void testPropertyAddDefaultLiteral_OnEdgeLabel() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B");
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                EdgeDefinition.of(),
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1)));
                }}
        );
        this.sqlgGraph.tx().commit();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Edge edge = a.addEdge("ab", b, "name", "a");
        this.sqlgGraph.tx().commit();

        Optional<PropertyColumn> column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        //add the check constraint
        this.topologyListenerTriple.clear();
        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1), "'b'"));
        this.sqlgGraph.tx().commit();
        a = this.sqlgGraph.addVertex(T.label, "A");
        b = this.sqlgGraph.addVertex(T.label, "B");
        a.addEdge("ab", b);
        this.sqlgGraph.tx().commit();

        List<Edge> edges = this.sqlgGraph.traversal().E().hasLabel("ab").has("name", "b").toList();
        Assert.assertEquals(1, edges.size());

        Assert.assertEquals(1, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(0).getRight());
        column1Optional = edgeLabel.getProperty("name");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        Assert.assertEquals(column1, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(1, column1.getPropertyDefinition().multiplicity().upper());

        column1.updatePropertyDefinition(PropertyDefinition.of(PropertyType.varChar(10), Multiplicity.of(1, 1)));
        List<String> defaultLiterals = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL).toList();
        Assert.assertEquals(0, defaultLiterals.size());
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            defaultLiterals = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL).toList();
            Assert.assertEquals(1, defaultLiterals.size());
            Assert.assertEquals("'b'", defaultLiterals.get(0));
        }
    }

}
