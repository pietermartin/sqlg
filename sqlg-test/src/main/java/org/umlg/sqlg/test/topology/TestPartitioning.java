package org.umlg.sqlg.test.topology;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/01/13
 */
public class TestPartitioning extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws PropertyVetoException, IOException, ClassNotFoundException {
        BaseTest.beforeClass();
        Assume.assumeTrue(isPostgres());
    }

    @Test
    public void testPartitioning() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel partitionedVertexLabel = publicSchema.ensurePartitionedVertexLabelExist("Measurement", new HashMap<String, PropertyType>() {{
                    put("date", PropertyType.LOCALDATE);
                    put("temp", PropertyType.INTEGER);
                }},
                PartitionType.RANGE,
                "date");
        partitionedVertexLabel.ensurePartitionExists("measurement1", "'2016-07-01'", "'2016-08-01'");
        partitionedVertexLabel.ensurePartitionExists("measurement2", "'2016-08-01'", "'2016-09-01'");
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
        partitionedVertexLabel.ensurePartitionExists("measurement2", "'2016-08-01'", "'2016-09-01'");
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

    @Test
    public void testPartitionedEdges() {
        VertexLabel person = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person");
        VertexLabel address = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Address");
        EdgeLabel livedAt = person.ensurePartitionedEdgeLabelExist("liveAt", address, new HashMap<String, PropertyType>() {{
            put("date", PropertyType.LOCALDATE);
        }}, PartitionType.RANGE, "date");
        Partition p1 = livedAt.ensurePartitionExists("livedAt1", "'2016-07-01'", "'2016-08-01'");
        Partition p2 = livedAt.ensurePartitionExists("livedAt2", "'2016-08-01'", "'2016-09-01'");
        this.sqlgGraph.tx().commit();

        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "Address");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "Address");
        person1.addEdge("liveAt", a1, "date", LocalDate.of(2016, 7, 1));
        person1.addEdge("liveAt", a2, "date", LocalDate.of(2016, 8, 2));
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraph.traversal().E().hasLabel("liveAt").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("liveAt").has("date", LocalDate.of(2016, 7, 1)).count().next(), 0);

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(2, this.sqlgGraph.traversal().E().hasLabel("liveAt").count().next(), 0);
            Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("liveAt").has("date", LocalDate.of(2016, 7, 1)).count().next(), 0);
            Assert.assertEquals(2, sqlgGraph1.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PARTITION).count().next(), 0);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

    }
}
