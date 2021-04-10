package org.umlg.sqlg.test.topology;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.test.BaseTest;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author Pieter Martin
 * Date: 2021/03/14
 */
public class TestLargeSchemaPerformance extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestLargeSchemaPerformance.class);

//    @Test
//    public void testSmallQueryLatency() {
//        LOGGER.info("started testSmallQueryLatency");
//        for (int i = 1; i <= 1_000_000; i++) {
//            List<Vertex> vertices = this.sqlgGraph.traversal().V()
//                    .has("R_5.T_100")
//                    .has("column10", P.eq("value_10"))
//                    .limit(1)
//                    .toList();
//            Assert.assertEquals(1, vertices.size());
//            if (i % 10 == 0) {
//                LOGGER.info(String.format("completed %d queries", i));
//            }
//        }
//    }
//
//    @Test
//    public void testInsertData() {
//        this.sqlgGraph.tx().streamingBatchModeOn();
//        LinkedHashMap<String, Object> values = values();
//        for (int i = 0; i < 1_000_000; i++) {
//            this.sqlgGraph.streamVertex("R_5.T_100", values);
//        }
//        this.sqlgGraph.tx().commit();
//    }

    @Test
    public void testPerformance() {
        Assume.assumeFalse(isMariaDb());
        //100 schemas
        //500 000 tables
        //100 columns in each table
        int numberOfSchemas = 10;
        StopWatch stopWatch = StopWatch.createStarted();

        for (int i = 1; i <= numberOfSchemas; i++) {
            this.sqlgGraph.getTopology().ensureSchemaExist("R_" + i);
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info(String.format("create schema %s", stopWatch.toString()));
        stopWatch = StopWatch.createStarted();
        StopWatch stopWatch2 = StopWatch.createStarted();
        int count = 1;
        for (int i = 1; i <= numberOfSchemas; i++) {
            Optional<Schema> schemaOptional = this.sqlgGraph.getTopology().getSchema("R_" + i);
            Assert.assertTrue(schemaOptional.isPresent());
            Schema schema = schemaOptional.get();
            for (int j = 1; j <= 1000; j++) {
                //100 columns
                schema.ensureVertexLabelExist("T" + j, columns());
                if (count % 1000 == 0) {
                    this.sqlgGraph.tx().commit();
                    stopWatch2.stop();
                    LOGGER.info(String.format("created %d for far in %s", count, stopWatch2.toString()));
                    stopWatch2.reset();
                    stopWatch2.start();
                }
                count++;
            }
            this.sqlgGraph.tx().commit();
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info(String.format("create table %s", stopWatch.toString()));
        Assert.assertEquals(numberOfSchemas + 1, sqlgGraph.getTopology().getSchemas().size());
        Assert.assertEquals(1000, sqlgGraph.getTopology().getSchema("R_1").orElseThrow().getVertexLabels().size());
        Assert.assertEquals(1000, sqlgGraph.getTopology().getSchema("R_2").orElseThrow().getVertexLabels().size());
        Assert.assertEquals(100, sqlgGraph.getTopology().getSchema("R_1").orElseThrow().getVertexLabel("T1").orElseThrow().getProperties().size());
        Assert.assertEquals(100, sqlgGraph.getTopology().getSchema("R_1").orElseThrow().getVertexLabel("T100").orElseThrow().getProperties().size());
        Assert.assertEquals(100, sqlgGraph.getTopology().getSchema("R_1").orElseThrow().getVertexLabel("T1000").orElseThrow().getProperties().size());
    }

    private Map<String, PropertyType> columns() {
        Map<String, PropertyType> result = new LinkedHashMap<>();
        for (int i = 1; i <= 100; i++) {
            result.put("column" + i, PropertyType.STRING);
        }
        return result;
    }

    private LinkedHashMap<String, Object> values() {
        LinkedHashMap<String, Object> result = new LinkedHashMap<>();
        for (int i = 1; i <= 100; i++) {
            result.put("column" + i, "value_" + i);
        }
        return result;
    }

}
