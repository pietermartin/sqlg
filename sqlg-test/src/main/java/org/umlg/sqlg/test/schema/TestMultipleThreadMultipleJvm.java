package org.umlg.sqlg.test.schema;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlSchemaChangeDialect;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
            Assume.assumeTrue(isPostgres());
            configuration.addProperty("distributed", true);
//            configuration.addProperty("maxPoolSize", 3);
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
        int NUMBER_OF_GRAPHS = 5;
        int NUMBER_OF_SCHEMAS = 100;
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
                                        //noinspection Duplicates
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

            for (Future<SqlgGraph> result : results) {
                result.get(100, TimeUnit.SECONDS);
            }
            Thread.sleep(1000);
            for (SqlgGraph graph : graphs) {
                assertEquals(this.sqlgGraph.getTopology(), graph.getTopology());
                for (Schema schema : graph.getTopology().getSchemas()) {
                    assertTrue(schema.isCommitted());
                }
            }
        } finally {
            for (SqlgGraph graph : graphs) {
                graph.close();
            }
        }
    }

    @Test
    public void testMultiThreadedSchemaCreation2() throws Exception {
        //number graphs, pretending its a separate jvm
        int NUMBER_OF_GRAPHS = 5;
        int NUMBER_OF_SCHEMAS = 100;
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

                for (int i = 0; i < NUMBER_OF_SCHEMAS; i++) {
                    final int count = i;
                    results.add(
                            poolPerGraphsExecutorCompletionService.submit(() -> {
                                        //noinspection Duplicates
                                        try {
                                            sqlgGraphAsync.getTopology().ensureSchemaExist("schema_" + count);
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
                                        return sqlgGraphAsync;
                                    }
                            )
                    );
                }
            }
            poolPerGraph.shutdown();

            for (Future<SqlgGraph> result : results) {
                result.get(100, TimeUnit.SECONDS);
            }
            Thread.sleep(1000);
            for (SqlgGraph graph : graphs) {
                assertEquals(this.sqlgGraph.getTopology(), graph.getTopology());
            }
        } finally {
            for (SqlgGraph graph : graphs) {
                graph.close();
            }
        }
    }

    @Test
    public void testMultiThreadedVertexLabelCreation() throws Exception {
        //number graphs, pretending its a separate jvm
        int NUMBER_OF_GRAPHS = 5;
        int NUMBER_OF_SCHEMAS = 100;
        //Pre-create all the graphs
        List<SqlgGraph> graphs = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_GRAPHS; i++) {
            graphs.add(SqlgGraph.open(configuration));
        }
        logger.info(String.format("Done firing up %d graphs", NUMBER_OF_GRAPHS));

        ExecutorService poolPerGraph = Executors.newFixedThreadPool(NUMBER_OF_GRAPHS);
        CompletionService<SqlgGraph> poolPerGraphsExecutorCompletionService = new ExecutorCompletionService<>(poolPerGraph);
        try {
            Map<String, PropertyType> properties = new HashMap<>();
            properties.put("name", PropertyType.STRING);
            properties.put("age", PropertyType.INTEGER);
            List<Future<SqlgGraph>> results = new ArrayList<>();
            for (final SqlgGraph sqlgGraphAsync : graphs) {
                for (int i = 0; i < NUMBER_OF_SCHEMAS; i++) {
                    final int count = i;
                    results.add(
                            poolPerGraphsExecutorCompletionService.submit(() -> {
                                        //noinspection Duplicates
                                        try {
                                            VertexLabel outVertexLabel = sqlgGraphAsync.getTopology().ensureVertexLabelExist("schema_" + count, "tableOut_" + count, properties);
                                            VertexLabel inVertexLabel = sqlgGraphAsync.getTopology().ensureVertexLabelExist("schema_" + count, "tableIn_" + count, properties);
                                            sqlgGraphAsync.getTopology().ensureEdgeLabelExist("edge_" + count, outVertexLabel, inVertexLabel, properties);
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
                                        return sqlgGraphAsync;
                                    }
                            )
                    );
                }
            }
            for (Future<SqlgGraph> result : results) {
                result.get(5, TimeUnit.MINUTES);
            }
            Thread.sleep(1000);
            for (SqlgGraph graph : graphs) {
                assertEquals(this.sqlgGraph.getTopology(), graph.getTopology());
                assertEquals(this.sqlgGraph.getTopology().toJson(), graph.getTopology().toJson());
            }
            logger.info("starting inserting data");

            for (final SqlgGraph sqlgGraphAsync : graphs) {
                for (int i = 0; i < NUMBER_OF_SCHEMAS; i++) {
                    final int count = i;
                    results.add(
                            poolPerGraphsExecutorCompletionService.submit(() -> {
                                        //noinspection Duplicates
                                        try {
                                            Vertex v1 = sqlgGraphAsync.addVertex(T.label, "schema_" + count + "." + "tableOut_" + count, "name", "asdasd", "age", 1);
                                            Vertex v2 = sqlgGraphAsync.addVertex(T.label, "schema_" + count + "." + "tableIn_" + count, "name", "asdasd", "age", 1);
                                            v1.addEdge("edge_" + count, v2, "name", "asdasd", "age", 1);
                                            final Random random = new Random();
                                            if (random.nextBoolean()) {
                                                sqlgGraphAsync.tx().rollback();
                                            } else {
                                                sqlgGraphAsync.tx().commit();
                                            }
                                        } catch (Exception e) {
                                            sqlgGraphAsync.tx().rollback();
                                            throw new RuntimeException(e);
                                        }
                                        return sqlgGraphAsync;
                                    }
                            )
                    );
                }
            }
            poolPerGraph.shutdown();

            for (Future<SqlgGraph> result : results) {
                result.get(30, TimeUnit.SECONDS);
            }
            //Because of the rollBack logic the insert code may also create topology elements, so sleep a bit for notify to do its thing.
            Thread.sleep(1000);
            logger.info("starting querying data");
            Set<Vertex> vertices = this.sqlgGraph.traversal().V().out().toSet();
            this.sqlgGraph.tx().rollback();
            for (SqlgGraph graph : graphs) {
                logger.info("assert querying data");
                Set<Vertex> actual = graph.traversal().V().out().toSet();
                logger.info("vertices.size = " + vertices.size() +  " actual.size = " + actual.size());
                assertEquals(vertices, actual);
                graph.tx().rollback();
            }
        } finally {
            for (SqlgGraph graph : graphs) {
                graph.close();
            }
        }
    }

    @Test
    public void testConcurrentModificationException() throws Exception {
        //number graphs, pretending its a separate jvm
        int NUMBER_OF_GRAPHS = 3;
        int NUMBER_OF_SCHEMAS = 100;
        //Pre-create all the graphs
        List<SqlgGraph> graphs = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_GRAPHS; i++) {
            graphs.add(SqlgGraph.open(configuration));
        }
        logger.info(String.format("Done firing up %d graphs", NUMBER_OF_GRAPHS));

        try {
            ExecutorService insertPoolPerGraph = Executors.newFixedThreadPool(NUMBER_OF_GRAPHS);
            CompletionService<SqlgGraph> insertPoolPerGraphsExecutorCompletionService = new ExecutorCompletionService<>(insertPoolPerGraph);
            List<Future<SqlgGraph>> results = new ArrayList<>();
            logger.info("starting inserting data");
            for (final SqlgGraph sqlgGraphAsync : graphs) {
                for (int i = 0; i < NUMBER_OF_SCHEMAS; i++) {
                    final int count = i;
                    results.add(
                            insertPoolPerGraphsExecutorCompletionService.submit(() -> {
                                        //noinspection Duplicates
                                        try {
                                            for (int j = 0; j < 10; j++) {
                                                Vertex v1 = sqlgGraphAsync.addVertex(T.label, "schema_" + count + "." + "tableOut_" + count, "name", "asdasd", "age", 1);
                                                Vertex v2 = sqlgGraphAsync.addVertex(T.label, "schema_" + count + "." + "tableIn_" + count, "name", "asdasd", "age", 1);
                                                v1.addEdge("edge_" + count, v2, "name", "asdasd", "age", 1);
                                                sqlgGraphAsync.tx().commit();
                                            }
                                        } catch (Exception e) {
                                            sqlgGraphAsync.tx().rollback();
                                            throw new RuntimeException(e);
                                        }
                                        return sqlgGraphAsync;
                                    }
                            )
                    );
                }
            }
            insertPoolPerGraph.shutdown();

            AtomicBoolean keepReading = new AtomicBoolean(true);
            ExecutorService readPoolPerGraph = Executors.newFixedThreadPool(NUMBER_OF_GRAPHS);
            CompletionService<SqlgGraph> readPoolPerGraphsExecutorCompletionService = new ExecutorCompletionService<>(readPoolPerGraph);
            List<Future<SqlgGraph>> readResults = new ArrayList<>();
            logger.info("starting reading data");
            for (final SqlgGraph sqlgGraphAsync : graphs) {
                for (int i = 0; i < NUMBER_OF_SCHEMAS; i++) {
                    readResults.add(
                            readPoolPerGraphsExecutorCompletionService.submit(() -> {
                                        //noinspection Duplicates
                                        try {
                                            while (keepReading.get()) {
                                                sqlgGraphAsync.getTopology().getAllTables();
                                                sqlgGraphAsync.getTopology().getAllEdgeForeignKeys();
                                                Thread.sleep(100);
                                            }
                                        } catch (Exception e) {
                                            sqlgGraphAsync.tx().rollback();
                                            throw new RuntimeException(e);
                                        }
                                        return sqlgGraphAsync;
                                    }
                            )
                    );
                }
            }
            readPoolPerGraph.shutdown();

            for (Future<SqlgGraph> result : results) {
                result.get(30, TimeUnit.SECONDS);
            }
            keepReading.set(false);
            for (Future<SqlgGraph> result : readResults) {
                result.get(30, TimeUnit.SECONDS);
            }
            logger.info("starting querying data");
            List<Vertex> vertices = this.sqlgGraph.traversal().V().out().toList();
            this.sqlgGraph.tx().rollback();
            for (SqlgGraph graph : graphs) {
                logger.info("assert querying data");
                assertEquals(vertices, graph.traversal().V().out().toList());
                graph.tx().rollback();
            }
        } finally {
            for (SqlgGraph graph : graphs) {
                graph.close();
            }
        }
    }

}
