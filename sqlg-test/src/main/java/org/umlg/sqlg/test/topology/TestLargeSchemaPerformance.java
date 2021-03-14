package org.umlg.sqlg.test.topology;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Assert;
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

    @Test
    public void testPerformance() throws InterruptedException {
        //100 schemas
        //500 000 tables
        //100 columns in each table
        int numberOfSchemas = 2;
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
        Assert.assertEquals(3, sqlgGraph.getTopology().getSchemas().size());
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

}
