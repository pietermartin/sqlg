package org.umlg.sqlg.test.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Assert;
import org.junit.Assume;
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

    private final static int NUMBER_OF_SCHEMAS = 10;
    private final static int NUMBER_OF_TABLES = 100;
    private final static int NUMBER_OF_PARTITIONS = 10;
    private final static int NUMBER_OF_COLUMNS = 100;

//    @Before
//    public void before() throws Exception {
//        System.out.println("before");
//    }
//
//    @Test
//    public void load() {
//        StopWatch stopWatch = StopWatch.createStarted();
//        this.sqlgGraph = SqlgGraph.open(configuration);
//        stopWatch.stop();
//        LOGGER.info("time taken: {}", stopWatch);
//    }

    @Test
    public void testPerformance() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsPartitioning());
        StopWatch stopWatch = StopWatch.createStarted();

        for (int i = 1; i <= NUMBER_OF_SCHEMAS; i++) {
            this.sqlgGraph.getTopology().ensureSchemaExist("R_" + i);
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("create schema {}", stopWatch);
        stopWatch = StopWatch.createStarted();
        StopWatch stopWatch2 = StopWatch.createStarted();
        int count = 1;
        for (int i = 1; i <= NUMBER_OF_SCHEMAS; i++) {
            Optional<Schema> schemaOptional = this.sqlgGraph.getTopology().getSchema("R_" + i);
            Assert.assertTrue(schemaOptional.isPresent());
            Schema schema = schemaOptional.get();
            for (int j = 1; j <= NUMBER_OF_TABLES; j++) {
                if (count % NUMBER_OF_TABLES == 0) {
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
                for (int k = 0; k < NUMBER_OF_PARTITIONS; k++) {
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
        Assert.assertEquals(NUMBER_OF_SCHEMAS + 1, sqlgGraph.getTopology().getSchemas().size());
        Assert.assertEquals(NUMBER_OF_TABLES, sqlgGraph.getTopology().getSchema("R_1").orElseThrow().getVertexLabels().size());
        Assert.assertEquals(NUMBER_OF_TABLES, sqlgGraph.getTopology().getSchema("R_2").orElseThrow().getVertexLabels().size());
        Assert.assertEquals(NUMBER_OF_COLUMNS, sqlgGraph.getTopology().getSchema("R_1").orElseThrow().getVertexLabel("T1").orElseThrow().getProperties().size());
        Assert.assertEquals(NUMBER_OF_COLUMNS, sqlgGraph.getTopology().getSchema("R_1").orElseThrow().getVertexLabel("T100").orElseThrow().getProperties().size());
//        Assert.assertEquals(NUMBER_OF_COLUMNS, sqlgGraph.getTopology().getSchema("R_1").orElseThrow().getVertexLabel("T1000").orElseThrow().getProperties().size());

        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(NUMBER_OF_SCHEMAS + 1, sqlgGraph1.getTopology().getSchemas().size());
            Assert.assertEquals(NUMBER_OF_TABLES, sqlgGraph1.getTopology().getSchema("R_1").orElseThrow().getVertexLabels().size());
            Assert.assertEquals(NUMBER_OF_TABLES, sqlgGraph1.getTopology().getSchema("R_2").orElseThrow().getVertexLabels().size());
            Assert.assertEquals(NUMBER_OF_COLUMNS, sqlgGraph1.getTopology().getSchema("R_1").orElseThrow().getVertexLabel("T1").orElseThrow().getProperties().size());
            Assert.assertEquals(NUMBER_OF_COLUMNS, sqlgGraph1.getTopology().getSchema("R_1").orElseThrow().getVertexLabel("T100").orElseThrow().getProperties().size());
//            Assert.assertEquals(NUMBER_OF_COLUMNS, sqlgGraph1.getTopology().getSchema("R_1").orElseThrow().getVertexLabel("T1000").orElseThrow().getProperties().size());

        }

    }

    private Map<String, PropertyDefinition> columns() {
        Map<String, PropertyDefinition> result = new LinkedHashMap<>();
        for (int i = 1; i <= NUMBER_OF_COLUMNS; i++) {
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
