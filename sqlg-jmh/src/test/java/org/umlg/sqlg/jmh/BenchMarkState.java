package org.umlg.sqlg.jmh;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.time.StopWatch;
import org.openjdk.jmh.annotations.*;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.util.SqlgUtil;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

@State(Scope.Benchmark)
public class BenchMarkState {

    SqlgGraph sqlgGraph;

    @Setup(Level.Invocation)
    public void setUp() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            Configuration configuration = configs.properties(sqlProperties);
            if (!configuration.containsKey("jdbc.url")) {
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
            }
            this.sqlgGraph = SqlgGraph.open(configuration);
            SqlgUtil.dropDb(this.sqlgGraph);
            this.sqlgGraph.tx().commit();
            this.sqlgGraph.close();
            this.sqlgGraph = SqlgGraph.open(configuration);
            this.sqlgGraph.getSqlDialect().grantReadOnlyUserPrivilegesToSqlgSchemas(this.sqlgGraph);
            this.sqlgGraph.tx().commit();

            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            Map<String, PropertyDefinition> columns = new HashMap<>();
            for (int i = 0; i < 100; i++) {
                columns.put("property_" + i, PropertyDefinition.of(PropertyType.STRING));
            }
            //Create a large schema, it slows the maps  down
//            int NUMBER_OF_SCHEMA_ELEMENTS = 1_000;
            int NUMBER_OF_SCHEMA_ELEMENTS = 10;
            for (int i = 0; i < NUMBER_OF_SCHEMA_ELEMENTS; i++) {
                VertexLabel person = this.sqlgGraph.getTopology().ensureVertexLabelExist("Person_" + i, columns);
                VertexLabel dog = this.sqlgGraph.getTopology().ensureVertexLabelExist("Dog_" + i, columns);
                person.ensureEdgeLabelExist("pet_" + i, dog, columns);
                if (i % 100 == 0) {
                    this.sqlgGraph.tx().commit();
                }
            }
            this.sqlgGraph.tx().commit();
//            LOGGER.info("done creating schema time taken: {}", stopWatch);
            stopWatch.reset();
            stopWatch.start();

            Map<String, Object> columnValues = new HashMap<>();
            for (int i = 0; i < 100; i++) {
                columnValues.put("property_" + i, "asdasdasd");
            }
            for (int i = 0; i < NUMBER_OF_SCHEMA_ELEMENTS; i++) {
                SqlgVertex person = (SqlgVertex) this.sqlgGraph.addVertex("Person_" + i, columnValues);
                SqlgVertex dog = (SqlgVertex) this.sqlgGraph.addVertex("Dog_" + i, columnValues);
                person.addEdgeWithMap("pet_" + i, dog, columnValues);
            }
            this.sqlgGraph.tx().commit();
            stopWatch.stop();
//            LOGGER.info("done inserting data time taken: {}", stopWatch);
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
        this.sqlgGraph.close();
    }
}
