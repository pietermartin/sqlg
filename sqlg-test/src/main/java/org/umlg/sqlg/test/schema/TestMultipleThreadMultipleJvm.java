package org.umlg.sqlg.test.schema;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlSchemaChangeDialect;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/10/29
 * Time: 7:24 PM
 */
public class TestMultipleThreadMultipleJvm extends BaseTest {

    private static final Logger logger = LoggerFactory.getLogger(TestMultipleThreadMultipleJvm.class.getName());

    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
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
        int NUMBER_OF_GRAPHS = 10;
        int NUMBER_OF_SCHEMAS = 200;
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
                                                logger.info("ensureSchemaExist " + "schema_" + i);
                                                sqlgGraphAsync.tx().commit();
                                            } else {
                                                sqlgGraphAsync.tx().rollback();
                                            }
                                        } catch (Exception e) {
                                            sqlgGraphAsync.tx().rollback();
                                            if (e.getCause().getClass().getSimpleName().equals("PSQLException")) {
                                                //swallow
                                                logger.warn("Rollback transaction due to schema creation failure.", e);
                                            } else {
                                                logger.error(String.format("got exception %s", e.getCause().getClass().getSimpleName()), e);
                                                throw new RuntimeException(e);
                                            }
                                        }
                                    }
                                    return sqlgGraphAsync;
                                }
                        )
                );
            }
            poolPerGraph.shutdown();
            for (Future<SqlgGraph> result : results) {
                result.get(1, TimeUnit.MINUTES);
            }
            Thread.sleep(10_000);
            for (SqlgGraph graph : graphs) {
                assertEquals(this.sqlgGraph.getTopology(), graph.getTopology());
                for (Schema schema : graph.getTopology().getSchemas()) {
                    Assert.assertTrue(schema.isCommitted());
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
                                            if (e.getCause().getClass().getSimpleName().equals("PSQLException")) {
                                                //swallow
                                                logger.warn("Rollback transaction due to schema creation failure.", e);
                                            } else {
                                                logger.error(String.format("got exception %s", e.getCause().getClass().getSimpleName()), e);
                                                throw new RuntimeException(e);
                                            }
                                        }
                                        return sqlgGraphAsync;
                                    }
                            )
                    );
                }
            }
            poolPerGraph.shutdown();

            for (Future<SqlgGraph> result : results) {
                result.get(1, TimeUnit.MINUTES);
            }
            Thread.sleep(10_000);
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
        //number of graphs, pretending they are in separate jvms
        int NUMBER_OF_GRAPHS = 5;
        int NUMBER_OF_SCHEMAS = 100;
        Set<Integer> successfulSchemas = new HashSet<>();
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
                                        for (int j = 0; j < 3; j++) {
                                            VertexLabel outVertexLabel;
                                            VertexLabel inVertexLabel;
                                            EdgeLabel edgeLabel;
                                            try {
                                                outVertexLabel = sqlgGraphAsync.getTopology().ensureVertexLabelExist("schema_" + count, "tableOut_" + count, properties);
                                                logger.info(String.format("created %s.%s", "schema_" + count, "tableOut_" + count));
                                                inVertexLabel = sqlgGraphAsync.getTopology().ensureVertexLabelExist("schema_" + count, "tableIn_" + count, properties);
                                                logger.info(String.format("created %s.%s", "schema_" + count, "tableIn_" + count));
                                                edgeLabel = sqlgGraphAsync.getTopology().ensureEdgeLabelExist("edge_" + count, outVertexLabel, inVertexLabel, properties);
                                                logger.info(String.format("created %s", "edge_" + count));
                                                Assert.assertNotNull(outVertexLabel);
                                                Assert.assertNotNull(inVertexLabel);
                                                Assert.assertNotNull(edgeLabel);
                                                final Random random = new Random();
                                                if (random.nextBoolean()) {
                                                    successfulSchemas.add(count);
                                                    sqlgGraphAsync.tx().commit();
                                                } else {
                                                    sqlgGraphAsync.tx().rollback();
                                                }
                                                break;
                                            } catch (Exception e) {
                                                sqlgGraphAsync.tx().rollback();
                                                if (e.getCause().getClass().getSimpleName().equals("PSQLException")) {
                                                    //swallow
                                                    logger.warn("Rollback transaction due to schema creation failure.", e);
                                                } else {
                                                    logger.error(String.format("got exception %s", e.getCause().getClass().getSimpleName()), e);
                                                    Assert.fail(e.getMessage());
                                                }
                                            }
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
            Thread.sleep(10_000);
            for (SqlgGraph graph : graphs) {
                assertEquals(this.sqlgGraph.getTopology(), graph.getTopology());
                logger.info(graph.getTopology().toJson().toString());
                assertEquals(successfulSchemas.size() + 1, this.sqlgGraph.getTopology().getSchemas().size());
                assertEquals(successfulSchemas.size() + 1, graph.getTopology().getSchemas().size());
                if (!this.sqlgGraph.getTopology().toJson().equals(graph.getTopology().toJson())) {
                    for (Schema schema : this.sqlgGraph.getTopology().getSchemas()) {
                        Optional<Schema> otherSchema = graph.getTopology().getSchema(schema.getName());
                        Assert.assertTrue(otherSchema.isPresent());
                        if (!schema.toJson().equals(otherSchema.get().toJson())) {
                            logger.debug(schema.toJson().toString());
                            logger.debug(otherSchema.get().toJson().toString());
                        }
                    }
                    Assert.fail("json not the same");
                }
            }
            logger.info("starting inserting data");

            for (final SqlgGraph sqlgGraphAsync : graphs) {
                for (int i = 0; i < NUMBER_OF_SCHEMAS; i++) {
                    if (successfulSchemas.contains(i)) {
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
            }
            poolPerGraph.shutdown();

            for (Future<SqlgGraph> result : results) {
                result.get(30, TimeUnit.SECONDS);
            }
            //Because of the rollBack logic the insert code may also create topology elements, so sleep a bit for notify to do its thing.
            Thread.sleep(10_000);
            logger.info("starting querying data");
            Set<Vertex> vertices = this.sqlgGraph.traversal().V().out().toSet();
            this.sqlgGraph.tx().rollback();
            for (SqlgGraph graph : graphs) {
                logger.info("assert querying data");
                Set<Vertex> actual = graph.traversal().V().out().toSet();
                logger.info("vertices.size = " + vertices.size() + " actual.size = " + actual.size());
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
                                            if (e.getCause() == null) {
                                                e.printStackTrace();
                                            }
                                            if (e.getCause().getClass().getSimpleName().equals("PSQLException")) {
                                                //swallow
                                                logger.warn("Rollback transaction due to schema creation failure.", e);
                                            } else {
                                                logger.error(String.format("got exception %s", e.getCause().getClass().getSimpleName()), e);
                                                Assert.fail(e.getMessage());
                                            }
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
                                        try {
                                            while (keepReading.get()) {
                                                sqlgGraphAsync.getTopology().getAllTables();
                                                sqlgGraphAsync.getTopology().getEdgeForeignKeys();
                                                //noinspection BusyWait
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
                SqlgGraph graph = result.get(30, TimeUnit.SECONDS);
                logger.info("graph results returned");
            }
            keepReading.set(false);
            for (Future<SqlgGraph> result : readResults) {
                SqlgGraph g = result.get(30, TimeUnit.SECONDS);
                logger.info("graph readResults returned");
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
