package org.umlg.sqlg.citus;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/04/03
 */
public class TestSharding extends BaseTest {

//    @Test
//    public void testSharding1() {
//        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
//        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist(
//                "A",
//                new HashMap<String, PropertyType>() {{
//                    put("uid", PropertyType.STRING);
//                    put("dist", PropertyType.STRING);
//                    put("value", PropertyType.STRING);
//                }},
//                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
//        );
//        aVertexLabel.distribute(8, "dist");
//        this.sqlgGraph.tx().commit();
//
//        CitusDialect citusDialect = (CitusDialect) this.sqlgGraph.getSqlDialect();
//        Assert.assertEquals(8, citusDialect.getShardCount(this.sqlgGraph, aVertexLabel));
//
//        char[] alphabet = "abcd".toCharArray();
//        this.sqlgGraph.tx().streamingBatchModeOn();
//        for (int i = 0; i < 1000; i++) {
//            int j = i % 4;
//            char x = alphabet[j];
//            this.sqlgGraph.streamVertex(T.label, "A.A", "uid", UUID.randomUUID().toString(), "dist", Character.toString(x), "value", Integer.toString(i));
//        }
//        this.sqlgGraph.tx().commit();
//
//        Assert.assertEquals(250, this.sqlgGraph.traversal().V().hasLabel("A.A").has("dist", "a").toList().size());
//
//    }

    @Test
    public void testSharding2() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        aVertexLabel.distribute(4, "dist");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.STRING);
                    put("dist", PropertyType.STRING);
                    put("value", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
        );
        bVertexLabel.distribute(4, "dist", aVertexLabel);

//        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist(
//                "ab",
//                bVertexLabel,
//                new HashMap<String, PropertyType>() {{
//                    put("uid", PropertyType.STRING);
//                    put("dist", PropertyType.STRING);
//                    put("value", PropertyType.STRING);
//                }},
//                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist"))
//        );
//        edgeLabel.distribute(4, "dist", aVertexLabel);
        this.sqlgGraph.tx().commit();

        List<String> tenantIds = Arrays.asList("RNC1", "RNC2", "RNC3", "RNC4");
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 10_000; i++) {
            int j = i % 4;
            String tenantId = tenantIds.get(j);
            Vertex a = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "dist", tenantId, "value", Integer.toString(i));
            Vertex b = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "dist", tenantId, "value", Integer.toString(i));
//            a.addEdge("ab", b, "uid", UUID.randomUUID().toString(), "dist", tenantId, "value", Integer.toString(i));
        }
        this.sqlgGraph.tx().commit();
    }

//    @Test
//    public void testShardingWithPartition() {
//        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
//                "A",
//                new HashMap<String, PropertyType>() {{
//                    put("uid", PropertyType.STRING);
//                    put("dist", PropertyType.STRING);
//                    put("date", PropertyType.LOCALDATE);
//                    put("value", PropertyType.STRING);
//                }},
//                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "dist")),
//                PartitionType.RANGE,
//                "date"
//        );
//        aVertexLabel.ensureRangePartitionExists("july", "'2016-07-01'", "'2016-08-01'");
//        aVertexLabel.ensureRangePartitionExists("august", "'2016-08-01'", "'2016-09-01'");
//        aVertexLabel.distribute(32, "dist");
//        this.sqlgGraph.tx().commit();
//
//        LocalDate localDate1 = LocalDate.of(2016, 7, 1);
//        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "dist", "a", "date", localDate1, "value", "1");
//        LocalDate localDate2 = LocalDate.of(2016, 8, 1);
//        this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "dist", "b", "date", localDate2, "value", "1");
//        this.sqlgGraph.tx().commit();
//
//    }
}
