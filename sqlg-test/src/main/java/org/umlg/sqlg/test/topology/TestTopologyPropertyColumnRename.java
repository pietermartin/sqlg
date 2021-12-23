package org.umlg.sqlg.test.topology;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

public class TestTopologyPropertyColumnRename extends BaseTest {

    private final List<Triple<TopologyInf, TopologyInf, TopologyChangeAction>> topologyListenerTriple = new ArrayList<>();

    @Before
    public void before() throws Exception {
        super.before();
        this.topologyListenerTriple.clear();
    }

    @Test
    public void testPropertyRename() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                    put("column1", PropertyType.varChar(10));
                }});
        this.sqlgGraph.tx().commit();
        Optional<VertexLabel> aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        Preconditions.checkState(aVertexLabelOptional.isPresent());
        VertexLabel aVertexLabel = aVertexLabelOptional.get();
        Optional<PropertyColumn> column1Optional = aVertexLabel.getProperty("column1");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        column1.rename("column2");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(1).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(1).getRight());
        Optional<PropertyColumn> column2Optional = aVertexLabel.getProperty("column2");
        Preconditions.checkState(column2Optional.isPresent());
        PropertyColumn column2 = column2Optional.get();
        Assert.assertEquals(column2, this.topologyListenerTriple.get(1).getLeft());

        List<String> propertyNames = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_NAME).toList();
        Assert.assertEquals(1, propertyNames.size());
        Assert.assertEquals("column2", propertyNames.get(0));

        column2.rename("column3");
        propertyNames = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_NAME).toList();
        Assert.assertEquals(1, propertyNames.size());
        Assert.assertEquals("column3", propertyNames.get(0));
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            propertyNames = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_NAME).toList();
            Assert.assertEquals(1, propertyNames.size());
            Assert.assertEquals("column2", propertyNames.get(0));
        }
    }

    @Test
    public void testPropertyOnEdgeLabelRename() {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>());
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("B", new LinkedHashMap<>());
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new HashMap<>() {{
            put("column1", PropertyType.STRING);
        }});
        this.sqlgGraph.tx().commit();
        Optional<EdgeLabel> edgeLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab");
        Preconditions.checkState(edgeLabelOptional.isPresent());
        EdgeLabel edgeLabel = edgeLabelOptional.get();
        Optional<PropertyColumn> column1Optional = edgeLabel.getProperty("column1");
        Preconditions.checkState(column1Optional.isPresent());
        PropertyColumn column1 = column1Optional.get();
        column1.rename("column2");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(4, this.topologyListenerTriple.size());
        Assert.assertNotEquals(column1, this.topologyListenerTriple.get(3).getLeft());
        Assert.assertEquals(column1, this.topologyListenerTriple.get(3).getMiddle());
        Assert.assertEquals(TopologyChangeAction.UPDATE, this.topologyListenerTriple.get(3).getRight());
        Optional<PropertyColumn> column2Optional = edgeLabel.getProperty("column2");
        Preconditions.checkState(column2Optional.isPresent());
        PropertyColumn column2 = column2Optional.get();
        Assert.assertEquals(column2, this.topologyListenerTriple.get(3).getLeft());

        List<String> propertyNames = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_NAME).toList();
        Assert.assertEquals(1, propertyNames.size());
        Assert.assertEquals("column2", propertyNames.get(0));

        column2.rename("column3");
        propertyNames = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_NAME).toList();
        Assert.assertEquals(1, propertyNames.size());
        Assert.assertEquals("column3", propertyNames.get(0));
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            propertyNames = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_NAME).toList();
            Assert.assertEquals(1, propertyNames.size());
            Assert.assertEquals("column2", propertyNames.get(0));
        }
    }

    @Test
    public void testRenameIdentifier() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                    put("column1", PropertyType.varChar(10));
                    put("column2", PropertyType.varChar(10));
                }}, ListOrderedSet.listOrderedSet(List.of("column1")));
        this.sqlgGraph.tx().commit();
        Optional<VertexLabel> aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        Preconditions.checkState(aVertexLabelOptional.isPresent());
        VertexLabel aVertexLabel = aVertexLabelOptional.get();
        Optional<PropertyColumn> column1Optional = aVertexLabel.getProperty("column1");
        Preconditions.checkState(column1Optional.isPresent());
        ListOrderedSet<String> identifiers = aVertexLabel.getIdentifiers();
        Assert.assertEquals(1, identifiers.size());
        Assert.assertEquals("column1", identifiers.get(0));
        PropertyColumn column1 = column1Optional.get();
        column1.rename("column1PK");
        this.sqlgGraph.tx().commit();

        List<Vertex> identifierProperties = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL)
                .out(Topology.SQLG_SCHEMA_VERTEX_IDENTIFIER_EDGE)
                .toList();
        Assert.assertEquals(1, identifierProperties.size());
        Assert.assertEquals("column1PK", identifierProperties.get(0).value(Topology.SQLG_SCHEMA_PROPERTY_NAME));

        aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        Preconditions.checkState(aVertexLabelOptional.isPresent());
        aVertexLabel = aVertexLabelOptional.get();
        column1Optional = aVertexLabel.getProperty("column1PK");
        Preconditions.checkState(column1Optional.isPresent());
        column1 = column1Optional.get();
        identifiers = aVertexLabel.getIdentifiers();
        Assert.assertEquals(1, identifiers.size());
        Assert.assertEquals("column1PK", identifiers.get(0));

        column1.rename("_column1PK");
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
            Preconditions.checkState(aVertexLabelOptional.isPresent());
            aVertexLabel = aVertexLabelOptional.get();
            column1Optional = aVertexLabel.getProperty("column1PK");
            Preconditions.checkState(column1Optional.isPresent());
            identifiers = aVertexLabel.getIdentifiers();
            Assert.assertEquals(1, identifiers.size());
            Assert.assertEquals("column1PK", identifiers.get(0));
        }
    }

    @Test
    public void testRenameIdentifierWithEdgeRoles() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A", new HashMap<>() {{
                    put("id1", PropertyType.varChar(10));
                    put("id2", PropertyType.varChar(10));
                    put("a", PropertyType.varChar(10));
                }},
                ListOrderedSet.listOrderedSet(List.of("id1", "id2"))
        );
        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B", new HashMap<>() {{
                    put("id1", PropertyType.varChar(10));
                    put("id2", PropertyType.varChar(10));
                    put("a", PropertyType.varChar(10));
                }},
                ListOrderedSet.listOrderedSet(List.of("id1", "id2"))
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel);
        this.sqlgGraph.tx().commit();

        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "id1", "1", "id2", "2", "a", "a");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "id1", "1", "id2", "2", "a", "a");
        a.addEdge("ab", b);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").out().toList();
        Assert.assertEquals(1, vertices.size());
        SchemaTable schemaTable = SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "A");
        vertices = this.sqlgGraph.traversal().V().hasId(P.eq(RecordId.from(schemaTable, List.of("1", "2")))).toList();
        Assert.assertEquals(1, vertices.size());

        Optional<PropertyColumn> id1Optional = aVertexLabel.getProperty("id1");
        Preconditions.checkState(id1Optional.isPresent());
        ListOrderedSet<String> identifiers = aVertexLabel.getIdentifiers();
        Assert.assertEquals(2, identifiers.size());
        Assert.assertEquals("id1", identifiers.get(0));
        PropertyColumn id1 = id1Optional.get();
        Assert.assertSame(id1Optional.get(), id1);
        id1.rename("id1PK");
        this.sqlgGraph.tx().commit();

        publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        Optional<VertexLabel> aVertexLabelOptional = publicSchema.getVertexLabel("A");
        Assert.assertTrue(aVertexLabelOptional.isPresent());
        Optional<VertexLabel> bVertexLabelOptional = publicSchema.getVertexLabel("B");
        Assert.assertTrue(bVertexLabelOptional.isPresent());
        Optional<EdgeLabel> edgeLabelOptional = publicSchema.getEdgeLabel("ab");
        Preconditions.checkState(edgeLabelOptional.isPresent());
        Set<EdgeRole> outEdgeRoles = edgeLabelOptional.get().getOutEdgeRoles();
        Assert.assertEquals(1, outEdgeRoles.size());
        EdgeRole edgeRole = outEdgeRoles.iterator().next();
        Assert.assertSame(aVertexLabelOptional.get(), edgeRole.getVertexLabel());
        Set<EdgeRole> inEdgeRoles = edgeLabelOptional.get().getInEdgeRoles();
        Assert.assertEquals(1, inEdgeRoles.size());
        edgeRole = inEdgeRoles.iterator().next();
        Assert.assertSame(bVertexLabelOptional.get(), edgeRole.getVertexLabel());

        vertices = this.sqlgGraph.traversal().V().hasLabel("A").out().toList();
        Assert.assertEquals(1, vertices.size());
        schemaTable = SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "A");
        vertices = this.sqlgGraph.traversal().V().hasId(P.eq(RecordId.from(schemaTable, List.of("1", "2")))).toList();
        Assert.assertEquals(1, vertices.size());

        id1Optional = aVertexLabel.getProperty("id1PK");
        Preconditions.checkState(id1Optional.isPresent());
        identifiers = aVertexLabel.getIdentifiers();
        Assert.assertEquals(2, identifiers.size());
        Assert.assertEquals("id1PK", identifiers.get(0));
        id1 = id1Optional.get();
        Assert.assertSame(id1Optional.get(), id1);
        id1.rename("id1PKAgain");
        this.sqlgGraph.tx().rollback();

        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
            aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
            Preconditions.checkState(aVertexLabelOptional.isPresent());
            aVertexLabel = aVertexLabelOptional.get();
            id1Optional = aVertexLabel.getProperty("id1PK");
            Preconditions.checkState(id1Optional.isPresent());
            identifiers = aVertexLabel.getIdentifiers();
            Assert.assertEquals(2, identifiers.size());
            Assert.assertEquals("id1PK", identifiers.get(0));
            Assert.assertEquals("id2", identifiers.get(1));
        }
    }

}
