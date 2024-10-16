package org.umlg.sqlg.test.recursive;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.LinkedHashMap;
import java.util.List;

public class TestTopologyRecursiveRepeat extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTopologyRecursiveRepeat.class);

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        Assume.assumeTrue(isPostgres());
    }

    @Test
    public void testTopologyPartitionRecursiveRepeat() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel measurement = publicSchema.ensurePartitionedVertexLabelExist("Measurement",
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                    put("list1", PropertyDefinition.of(PropertyType.STRING));
                    put("list2", PropertyDefinition.of(PropertyType.INTEGER));
                    put("unitsales", PropertyDefinition.of(PropertyType.INTEGER));
                }},
                ListOrderedSet.listOrderedSet(List.of("name", "list1", "list2")),
                PartitionType.LIST,
                "list1");

        Partition p1 = measurement.ensureListPartitionWithSubPartitionExists("measurement_list1", "'1'", PartitionType.LIST, "list2");
        Partition p2 = measurement.ensureListPartitionWithSubPartitionExists("measurement_list2", "'2'", PartitionType.LIST, "list2");
        p1.ensureListPartitionExists("measurement_list1_1", "1");
        p1.ensureListPartitionExists("measurement_list1_2", "2");
        p1.ensureListPartitionExists("measurement_list1_3", "3");
        p1.ensureListPartitionExists("measurement_list1_4", "4");

        p2.ensureListPartitionExists("measurement_list2_1", "1");
        p2.ensureListPartitionExists("measurement_list2_2", "2");
        p2.ensureListPartitionExists("measurement_list2_3", "3");
        p2.ensureListPartitionExists("measurement_list2_4", "4");
        this.sqlgGraph.tx().commit();

        List<Object> partitionIds = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL)
                .out(Topology.SQLG_SCHEMA_VERTEX_PARTITION_EDGE)
                .id()
                .toList();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>)this.sqlgGraph.topology().V().hasId(P.within(partitionIds))
                .repeat(
                        __.out(Topology.SQLG_SCHEMA_PARTITION_PARTITION_EDGE).simplePath()
                )
                .until(
                        __.not(__.out(Topology.SQLG_SCHEMA_PARTITION_PARTITION_EDGE).simplePath())
                )
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        Assert.assertEquals(8, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(c) && p.get(3).equals(d)));

    }

}
