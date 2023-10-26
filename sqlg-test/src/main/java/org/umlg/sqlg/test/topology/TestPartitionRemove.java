package org.umlg.sqlg.test.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.LinkedHashMap;
import java.util.List;

public class TestPartitionRemove extends BaseTest {

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
    }
}
