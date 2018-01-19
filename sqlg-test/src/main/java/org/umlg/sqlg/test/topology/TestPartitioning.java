package org.umlg.sqlg.test.topology;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.time.LocalDate;
import java.util.HashMap;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/01/13
 */
public class TestPartitioning extends BaseTest {

//    @Test
    public void testPartitioning() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel partitionedVertexLabel = publicSchema.ensurePartitionedVertexLabelExist("Measurement", new HashMap<String, PropertyType>() {{
                    put("date", PropertyType.LOCALDATE);
                    put("temp", PropertyType.INTEGER);
                }},
                PartitionType.RANGE,
                "date");
        partitionedVertexLabel.ensurePartitionExists("measurement1", "'2016-07-01'", "'2016-08-01'");
        partitionedVertexLabel.ensurePartitionExists("measurement2", "'2016-08-01'",  "'2016-09-01'");
        this.sqlgGraph.tx().commit();

        LocalDate localDate1 = LocalDate.of(2016, 7, 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "date", localDate1);
        LocalDate localDate2 = LocalDate.of(2016, 8, 1);
        this.sqlgGraph.addVertex(T.label, "Measurement", "date", localDate2);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("Measurement").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Measurement").has("date", localDate1).count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Measurement").has("date", localDate2).count().next(), 0);

        Partition partition = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Measurement").get().getPartition("measurement1").get();
        partition.remove();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Measurement").count().next(), 0);
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("Measurement").has("date", localDate1).count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Measurement").has("date", localDate2).count().next(), 0);

        Assert.assertEquals(1, this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PARTITION).count().next(), 0);
    }

    @Test
    public void testPartitionInSchema() {
        Schema testSchema = this.sqlgGraph.getTopology().ensureSchemaExist("test");
        VertexLabel partitionedVertexLabel = testSchema.ensurePartitionedVertexLabelExist("Measurement", new HashMap<String, PropertyType>() {{
                    put("date", PropertyType.LOCALDATE);
                    put("temp", PropertyType.INTEGER);
                }},
                PartitionType.RANGE,
                "date");
        partitionedVertexLabel.ensurePartitionExists("measurement1", "'2016-07-01'", "'2016-08-01'");
        partitionedVertexLabel.ensurePartitionExists("measurement2", "'2016-08-01'",  "'2016-09-01'");
        this.sqlgGraph.tx().commit();

        LocalDate localDate1 = LocalDate.of(2016, 7, 1);
        this.sqlgGraph.addVertex(T.label, "test.Measurement", "date", localDate1);
        LocalDate localDate2 = LocalDate.of(2016, 8, 1);
        this.sqlgGraph.addVertex(T.label, "test.Measurement", "date", localDate2);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("test.Measurement").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("test.Measurement").has("date", localDate1).count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("test.Measurement").has("date", localDate2).count().next(), 0);

        Partition partition = this.sqlgGraph.getTopology().getSchema("test").get().getVertexLabel("Measurement").get().getPartition("measurement1").get();
        partition.remove();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("test.Measurement").count().next(), 0);
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("test.Measurement").has("date", localDate1).count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("test.Measurement").has("date", localDate2).count().next(), 0);

        Assert.assertEquals(1, this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PARTITION).count().next(), 0);

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {

            Assert.assertEquals(1, sqlgGraph1.traversal().V().hasLabel("test.Measurement").count().next(), 0);
            Assert.assertEquals(0, sqlgGraph1.traversal().V().hasLabel("test.Measurement").has("date", localDate1).count().next(), 0);
            Assert.assertEquals(1, sqlgGraph1.traversal().V().hasLabel("test.Measurement").has("date", localDate2).count().next(), 0);

            Assert.assertEquals(1, sqlgGraph1.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PARTITION).count().next(), 0);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

    }
}
