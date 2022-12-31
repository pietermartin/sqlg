package org.umlg.sqlg.test.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.PartitionType;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Pieter Martin
 * Date: 2021/03/14
 */
public class TestLargeSchemaPerformance extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestLargeSchemaPerformance.class);

    @Before
    public void before() throws Exception {
        System.out.println("before");
    }

    @Test
    public void load() {
        StopWatch stopWatch = StopWatch.createStarted();
        this.sqlgGraph = SqlgGraph.open(configuration);
        stopWatch.stop();
        LOGGER.info("time taken: {}", stopWatch);
    }

//    @Test
    public void testPerformance() {
//        this.sqlgGraph = SqlgGraph.open(configuration);
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
        LOGGER.info("create schema {}", stopWatch);
        stopWatch = StopWatch.createStarted();
        StopWatch stopWatch2 = StopWatch.createStarted();
        int count = 1;
        for (int i = 1; i <= numberOfSchemas; i++) {
            Optional<Schema> schemaOptional = this.sqlgGraph.getTopology().getSchema("R_" + i);
            Assert.assertTrue(schemaOptional.isPresent());
            Schema schema = schemaOptional.get();
            for (int j = 1; j <= 1000; j++) {
                //100 columns
//                schema.ensureVertexLabelExist("T" + j, columns());
                if (count % 1000 == 0) {
                    this.sqlgGraph.tx().commit();
                    stopWatch2.stop();
                    LOGGER.info("created {} for far in {}", count, stopWatch2);
                    stopWatch2.reset();
                    stopWatch2.start();
                }
                VertexLabel vertexLabel = schema.ensurePartitionedVertexLabelExist(
                        "T" + j,
                        columns(),
                        ListOrderedSet.listOrderedSet(List.of("column1")),
                        PartitionType.LIST,
                        "column1"
                );
                for (int k = 0; k < 10; k++) {
                    vertexLabel.ensureListPartitionExists(
                            "test" + j + k,
                            "'test" + j + k + "'"
                    );
                }
                count++;
            }
            this.sqlgGraph.tx().commit();
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("create table {}", stopWatch);
        Assert.assertEquals(numberOfSchemas + 1, sqlgGraph.getTopology().getSchemas().size());
        Assert.assertEquals(1000, sqlgGraph.getTopology().getSchema("R_1").orElseThrow().getVertexLabels().size());
        Assert.assertEquals(1000, sqlgGraph.getTopology().getSchema("R_2").orElseThrow().getVertexLabels().size());
        Assert.assertEquals(100, sqlgGraph.getTopology().getSchema("R_1").orElseThrow().getVertexLabel("T1").orElseThrow().getProperties().size());
        Assert.assertEquals(100, sqlgGraph.getTopology().getSchema("R_1").orElseThrow().getVertexLabel("T100").orElseThrow().getProperties().size());
        Assert.assertEquals(100, sqlgGraph.getTopology().getSchema("R_1").orElseThrow().getVertexLabel("T1000").orElseThrow().getProperties().size());

        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(numberOfSchemas + 1, sqlgGraph1.getTopology().getSchemas().size());
            Assert.assertEquals(1000, sqlgGraph1.getTopology().getSchema("R_1").orElseThrow().getVertexLabels().size());
            Assert.assertEquals(1000, sqlgGraph1.getTopology().getSchema("R_2").orElseThrow().getVertexLabels().size());
            Assert.assertEquals(100, sqlgGraph1.getTopology().getSchema("R_1").orElseThrow().getVertexLabel("T1").orElseThrow().getProperties().size());
            Assert.assertEquals(100, sqlgGraph1.getTopology().getSchema("R_1").orElseThrow().getVertexLabel("T100").orElseThrow().getProperties().size());
            Assert.assertEquals(100, sqlgGraph1.getTopology().getSchema("R_1").orElseThrow().getVertexLabel("T1000").orElseThrow().getProperties().size());

        }

    }

    private Map<String, PropertyDefinition> columns() {
        Map<String, PropertyDefinition> result = new LinkedHashMap<>();
        for (int i = 1; i <= 100; i++) {
            result.put("column" + i, PropertyDefinition.of(PropertyType.STRING));
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
