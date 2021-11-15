package org.umlg.sqlg.test.topology;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

public class TestTopologyRename extends BaseTest {

    @Test
    public void testPropertyRename() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                    put("column1", PropertyType.STRING);
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

        List<String> propertyNames = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_NAME).toList();
        Assert.assertEquals(1, propertyNames.size());
        Assert.assertEquals("column2", propertyNames.get(0));

        Optional<PropertyColumn> column2Optional = aVertexLabel.getProperty("column2");
        Preconditions.checkState(column2Optional.isPresent());
        PropertyColumn column2 = column2Optional.get();

        column2.rename("column3");
        propertyNames = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_NAME).toList();
        Assert.assertEquals(1, propertyNames.size());
        Assert.assertEquals("column3", propertyNames.get(0));
        this.sqlgGraph.tx().rollback();

        if (isPostgres() || isMsSqlServer()) {
            propertyNames = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_NAME).toList();
            Assert.assertEquals(1, propertyNames.size());
            Assert.assertEquals("column2", propertyNames.get(0));
        }
    }

    @Test
    public void testRenameIdentifier() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                    put("column1", PropertyType.STRING);
                    put("column2", PropertyType.STRING);
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

        if (isPostgres() || isMsSqlServer()) {
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

}
