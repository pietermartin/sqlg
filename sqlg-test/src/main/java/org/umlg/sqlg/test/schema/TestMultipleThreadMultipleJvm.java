package org.umlg.sqlg.test.schema;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlSchemaChangeDialect;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;
import org.umlg.sqlg.topology.Schema;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/10/29
 * Time: 7:24 PM
 */
public class TestMultipleThreadMultipleJvm extends BaseTest {

    private static Logger logger = LoggerFactory.getLogger(TestMultipleThreadMultipleJvm.class.getName());

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
            Assume.assumeTrue(configuration.getString("jdbc.url").contains("postgresql"));
            configuration.addProperty("distributed", true);
            configuration.addProperty("maxPoolSize", 5);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

        @Test
    public void testMultiThreadedLocking() throws Exception {
        //number graphs, pretending its a separate jvm
        int NUMBER_OF_GRAPHS = 10;
        ExecutorService sqlgGraphsExecutorService = Executors.newFixedThreadPool(100);
        CompletionService<Boolean> sqlgGraphsExecutorCompletionService = new ExecutorCompletionService<>(sqlgGraphsExecutorService);
        List<SqlgGraph> graphs = new ArrayList<>();
        try {
            //Pre-create all the graphs
            for (int i = 0; i < NUMBER_OF_GRAPHS; i++) {
                graphs.add(SqlgGraph.open(configuration));
            }
            List<Future<Boolean>> results = new ArrayList<>();
            for (SqlgGraph sqlgGraphAsync : graphs) {
                results.add(sqlgGraphsExecutorCompletionService.submit(() -> {
                    ((SqlSchemaChangeDialect) sqlgGraphAsync.getSqlDialect()).lock(sqlgGraphAsync);
                    sqlgGraphAsync.tx().rollback();
                    return true;
                }));
            }
            sqlgGraphsExecutorService.shutdown();
            for (Future<Boolean> result : results) {
                result.get(10, TimeUnit.SECONDS);
            }
        } finally {
            for (SqlgGraph graph : graphs) {
                graph.close();
            }
        }
    }

    @Test
    public void testMultiThreadedSchemaCreation() throws Exception {
        //number graphs, pretending its a separate jvm
        int NUMBER_OF_GRAPHS = 19;
        int NUMBER_OF_SCHEMAS = 1000;
        //Pre-create all the graphs
        List<SqlgGraph> graphs = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_GRAPHS; i++) {
            graphs.add(SqlgGraph.open(configuration));
        }
        logger.info(String.format("Done firing up %d graphs", NUMBER_OF_GRAPHS));

        ExecutorService poolPerGraph = Executors.newFixedThreadPool(NUMBER_OF_GRAPHS);
        CompletionService<SqlgGraph> poolPerGraphsExecutorCompletionService = new ExecutorCompletionService<>(poolPerGraph);
        try {

            List<Future<SqlgGraph>> results = new ArrayList<>();
            for (final SqlgGraph sqlgGraphAsync : graphs) {

                results.add(
                        poolPerGraphsExecutorCompletionService.submit(() -> {
                                    for (int i = 0; i < NUMBER_OF_SCHEMAS; i++) {
                                        try {
                                            sqlgGraphAsync.getTopology().ensureSchemaExist("schema_" + i);
                                            final Random random = new Random();
                                            if (random.nextBoolean()) {
                                                sqlgGraphAsync.tx().commit();
                                            } else {
                                                sqlgGraphAsync.tx().rollback();
                                            }
                                        } catch (Exception e) {
                                            sqlgGraphAsync.tx().rollback();
                                            throw new RuntimeException(e);
                                        }
                                    }
                                    return sqlgGraphAsync;
                                }
                        )
                );
            }
            poolPerGraph.shutdown();

            List<Set<Schema>>  schemas = new ArrayList<>();
            for (Future<SqlgGraph> result : results) {
                SqlgGraph sqlgGraph = result.get(100, TimeUnit.SECONDS);
                schemas.add(sqlgGraph.getTopology().getSchemas());
            }
            Thread.sleep(1000);
            Set<Schema> rootGraphSchemas = this.sqlgGraph.getTopology().getSchemas();
            assertEquals(NUMBER_OF_GRAPHS, schemas.size());
            for (Set<Schema> schema : schemas) {
                assertEquals(rootGraphSchemas, schema);
            }
        } finally {
            for (SqlgGraph graph : graphs) {
                graph.close();
            }
        }
    }

}
