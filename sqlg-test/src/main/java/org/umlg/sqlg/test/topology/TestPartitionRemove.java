package org.umlg.sqlg.test.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;

public class TestPartitionRemove extends BaseTest {

    @Test
    public void testRemoveSubPartitionAndRecreateWithSameName() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsPartitioning());
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel aVertexLabel = aSchema.ensurePartitionedVertexLabelExist("A",
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                    put("date", PropertyDefinition.of(PropertyType.LOCALDATE));
                }},
                ListOrderedSet.listOrderedSet(List.of("name", "date")),
                PartitionType.LIST,
                "\"name\""
        );
        Partition a1 = aVertexLabel.ensureListPartitionWithSubPartitionExists("a1", "'a1'", PartitionType.RANGE, "\"date\"");
        a1.ensureRangePartitionExists("a1_oct", "'2025-10-01'", "'2025-11-01'");
        this.sqlgGraph.tx().commit();

        a1.getPartition("a1_oct").orElseThrow().remove();
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(a1.getPartition("a1_oct").isEmpty());
        Assert.assertTrue(aVertexLabel.getPartition("a1").orElseThrow().getPartitions().isEmpty());

        //the name can be used again, for a different range
        a1.ensureRangePartitionExists("a1_oct", "'2026-10-01'", "'2026-11-01'");
        this.sqlgGraph.tx().commit();
        Partition oct = a1.getPartition("a1_oct").orElseThrow();
        Assert.assertEquals("'2026-10-01'", oct.getFrom());
        Assert.assertEquals("'2026-11-01'", oct.getTo());
        this.sqlgGraph.addVertex(T.label, "A.A", "name", "a1", "date", LocalDate.of(2026, 10, 8));
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A.A").count().next(), 0);
    }

    @Test
    public void testRemovePartitionViaVertexLabelRemove() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel aVertexLabel = aSchema.ensurePartitionedVertexLabelExist("A",
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                    put("part1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("part2", PropertyDefinition.of(PropertyType.INTEGER));
                    put("part3", PropertyDefinition.of(PropertyType.INTEGER));
                    put("other", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(List.of("name", "part1", "part2", "part3")),
                PartitionType.LIST,
                "\"part1\""
        );
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B");
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel);

        Partition part1_1 = aVertexLabel.ensureListPartitionWithSubPartitionExists("part1_1", "'1'", PartitionType.LIST, "\"part2\"");
        Partition part1_2 = aVertexLabel.ensureListPartitionWithSubPartitionExists("part1_2", "'2'", PartitionType.LIST, "\"part2\"");

        Partition part1_1_1 = part1_1.ensureListPartitionWithSubPartitionExists("part1_1_1", "1", PartitionType.LIST, "\"part3\"");
        part1_1_1.ensureListPartitionExists("part1_1_1_1", "1");
        part1_1_1.ensureListPartitionExists("part1_1_1_2", "2");
        Partition part1_1_2 = part1_1.ensureListPartitionWithSubPartitionExists("part1_1_2", "2", PartitionType.LIST, "\"part3\"");
        part1_1_2.ensureListPartitionExists("part1_1_2_1", "1");
        part1_1_2.ensureListPartitionExists("part1_1_2_2", "2");


        Partition part1_2_1 = part1_2.ensureListPartitionWithSubPartitionExists("part1_2_1", "1", PartitionType.LIST, "\"part3\"");
        Partition part1_2_2 = part1_2.ensureListPartitionWithSubPartitionExists("part1_2_2", "2", PartitionType.LIST, "\"part3\"");
        part1_2_1.ensureListPartitionExists("part1_2_1_1", "1");
        part1_2_2.ensureListPartitionExists("part1_2_2_1", "1");
        this.sqlgGraph.tx().commit();

        aVertexLabel.remove();
        this.sqlgGraph.tx().commit();

        Assert.assertFalse(this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL)
                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, P.eq("A"))
                .hasNext());
        Assert.assertFalse(this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_EDGE_LABEL).hasNext());
        Assert.assertFalse(this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).hasNext());
        Assert.assertFalse(this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_INDEX).hasNext());
        Assert.assertFalse(this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PARTITION)
                .has(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PARTITION_ABSTRACT_LABEL_NAME, P.eq("A"))
                .hasNext());

        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("select * from " + Topology.SQLG_SCHEMA + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.VERTEX_PREFIX + Topology.SQLG_SCHEMA_PARTITION));
            Assert.assertFalse(rs.next());
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRemovePartitionViaSchemaRemove() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel aVertexLabel = aSchema.ensurePartitionedVertexLabelExist("A",
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                    put("part1", PropertyDefinition.of(PropertyType.INTEGER));
                    put("part2", PropertyDefinition.of(PropertyType.INTEGER));
                    put("part3", PropertyDefinition.of(PropertyType.INTEGER));
                    put("other", PropertyDefinition.of(PropertyType.STRING));
                }},
                ListOrderedSet.listOrderedSet(List.of("name", "part1", "part2", "part3")),
                PartitionType.LIST,
                "\"part1\""
        );
        VertexLabel bVertexLabel = aSchema.ensureVertexLabelExist("B");
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel);

        Partition part1_1 = aVertexLabel.ensureListPartitionWithSubPartitionExists("part1_1", "'1'", PartitionType.LIST, "\"part2\"");
        Partition part1_2 = aVertexLabel.ensureListPartitionWithSubPartitionExists("part1_2", "'2'", PartitionType.LIST, "\"part2\"");

        Partition part1_1_1 = part1_1.ensureListPartitionWithSubPartitionExists("part1_1_1", "1", PartitionType.LIST, "\"part3\"");
        part1_1_1.ensureListPartitionExists("part1_1_1_1", "1");
        part1_1_1.ensureListPartitionExists("part1_1_1_2", "2");
        Partition part1_1_2 = part1_1.ensureListPartitionWithSubPartitionExists("part1_1_2", "2", PartitionType.LIST, "\"part3\"");
        part1_1_2.ensureListPartitionExists("part1_1_2_1", "1");
        part1_1_2.ensureListPartitionExists("part1_1_2_2", "2");


        Partition part1_2_1 = part1_2.ensureListPartitionWithSubPartitionExists("part1_2_1", "1", PartitionType.LIST, "\"part3\"");
        Partition part1_2_2 = part1_2.ensureListPartitionWithSubPartitionExists("part1_2_2", "2", PartitionType.LIST, "\"part3\"");
        part1_2_1.ensureListPartitionExists("part1_2_1_1", "1");
        part1_2_2.ensureListPartitionExists("part1_2_2_1", "1");
        this.sqlgGraph.tx().commit();

        aSchema.remove();
        this.sqlgGraph.tx().commit();

        Assert.assertFalse(this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .has(Topology.SQLG_SCHEMA_SCHEMA_NAME, P.neq(sqlgGraph.getSqlDialect().getPublicSchema()))
                .hasNext());
        Assert.assertFalse(this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL).hasNext());
        Assert.assertFalse(this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_EDGE_LABEL).hasNext());
        Assert.assertFalse(this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).hasNext());
        Assert.assertFalse(this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_INDEX).hasNext());
        Assert.assertFalse(this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PARTITION).hasNext());
        
        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("select * from " + Topology.SQLG_SCHEMA + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.VERTEX_PREFIX + Topology.SQLG_SCHEMA_PARTITION));
            Assert.assertFalse(rs.next());
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }
}
