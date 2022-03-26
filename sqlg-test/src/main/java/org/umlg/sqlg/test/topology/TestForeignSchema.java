package org.umlg.sqlg.test.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;
import org.umlg.sqlg.util.SqlgUtil;

import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@SuppressWarnings({"rawtypes", "DuplicatedCode"})
public class TestForeignSchema extends BaseTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(TestForeignSchema.class);
    private SqlgGraph sqlgGraphFdw;

    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            Assume.assumeTrue(isPostgres());
            configuration.addProperty("distributed", true);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (isPostgres()) {
            URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
            try {
                Configurations configs = new Configurations();
                configuration = configs.properties(sqlProperties);
                SqlgGraph sqlgGraph = SqlgGraph.open(configuration);
                Connection connection = sqlgGraph.tx().getConnection();
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DROP SERVER IF EXISTS sqlgraph_fwd_server CASCADE;");
                }
                sqlgGraph.tx().commit();
            } catch (ConfigurationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Before
    public void before() throws Exception {
        Assume.assumeTrue(isPostgres());
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph = SqlgGraph.open(configuration);
        SqlgUtil.dropDb(this.sqlgGraph);
        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            for (String table : List.of("V_A", "V_B", "E_ab", "E_ba",
                    "V_person", "V_software", "E_created", "E_knows",
                    "V_song", "V_artist", "E_followedBy", "E_writtenBy", "E_sungBy")) {
                String sql = String.format(
                        "DROP FOREIGN TABLE IF EXISTS \"%s\" CASCADE;",
                        table
                );
                statement.execute(sql);
            }
            String sql = String.format(
                    "DROP SCHEMA IF EXISTS \"%s\" CASCADE;",
                    "B"
            );
            statement.execute(sql);
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);

        this.sqlgGraphFdw = SqlgGraph.open("sqlg.fdw.properties");
        SqlgUtil.dropDb(this.sqlgGraphFdw);
        this.sqlgGraphFdw.tx().commit();
        this.sqlgGraphFdw.close();
        this.sqlgGraphFdw = SqlgGraph.open("sqlg.fdw.properties");


        grantReadOnlyUserPrivileges();
        assertNotNull(this.sqlgGraph);
        assertNotNull(this.sqlgGraph.getBuildVersion());
        this.gt = this.sqlgGraph.traversal();
        if (configuration.getBoolean("distributed", false)) {
            this.sqlgGraph1 = SqlgGraph.open(configuration);
            assertNotNull(this.sqlgGraph1);
            assertEquals(this.sqlgGraph.getBuildVersion(), this.sqlgGraph1.getBuildVersion());
        }
        stopWatch.stop();
        LOGGER.info("Startup time for test = " + stopWatch);
        connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw;");
            String sql = String.format(
                    "CREATE SERVER \"%s\" FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '%s', dbname '%s', port '%d');",
                    "sqlgraph_fwd_server",
                    "localhost",
                    "sqlgraphdb_fdw",
                    5432
            );
            statement.execute(sql);
            this.sqlgGraph.tx().commit();
        } catch (Exception e) {
            //swallow
//            LOGGER.error("Failed to create 'sqlgraph_fwd_server'", e);
        } finally {
            this.sqlgGraph.tx().rollback();
        }
        connection = sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            String sql = String.format(
                    "CREATE USER MAPPING FOR %s SERVER \"%s\" OPTIONS (user '%s', password '%s');",
                    "postgres",
                    "sqlgraph_fwd_server",
                    "postgres",
                    "postgres"
            );
            statement.execute(sql);
            this.sqlgGraph.tx().commit();
        } catch (SQLException throwables) {
            //swallow for mostly the server will already exist
        } finally {
            this.sqlgGraph.tx().rollback();
        }
    }

    @After
    public void after() {
        try {
            this.sqlgGraph.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);
            this.sqlgGraph.close();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        try {
            if (this.sqlgGraph1 != null) {
                this.sqlgGraph1.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);
                this.sqlgGraph1.close();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        try {
            if (this.sqlgGraphFdw != null) {
                this.sqlgGraphFdw.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);
                this.sqlgGraphFdw.close();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Test
    public void testImportForeignSchemaVertexLabels() throws SQLException {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        this.sqlgGraph.tx().commit();

        Schema b = this.sqlgGraphFdw.getTopology().ensureSchemaExist("B");
        @SuppressWarnings("UnusedAssignment")
        VertexLabel bVertexLabel = b.ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("b", PropertyType.STRING);
                }}
        );
        this.sqlgGraphFdw.tx().commit();

        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            String sql = String.format(
                    "CREATE SCHEMA \"%s\";",
                    "B"
            );
            statement.execute(sql);
            sql = String.format(
                    "IMPORT FOREIGN SCHEMA \"%s\" FROM SERVER \"%s\" INTO \"%s\";",
                    "B",
                    "sqlgraph_fwd_server",
                    "B"
            );
            statement.execute(sql);
        }
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.getTopology().importForeignSchemas(Set.of(this.sqlgGraphFdw.getTopology().getSchema("B").orElseThrow()));
        Assert.assertTrue(this.sqlgGraph.getTopology().getSchema("B").isPresent());

        //Check its readOnly
        Schema foreignSchema = this.sqlgGraph.getTopology().getSchema("B").orElseThrow();
        boolean failed = false;
        try {
            foreignSchema.ensureVertexLabelExist("D",
                    new LinkedHashMap<>() {{
                        put("d", PropertyType.STRING);
                    }});
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalStateException);
            Assert.assertEquals("'B' is a read only foreign schema!", e.getMessage());
            failed = true;
        }
        Assert.assertTrue(failed);
        failed = false;
        try {
            bVertexLabel = foreignSchema.getVertexLabel("B").orElseThrow();
            Map<String, PropertyColumn> properties = bVertexLabel.getProperties();
            Map<String, PropertyType> propertyTypeMap = new HashMap<>();
            for (String p : properties.keySet()) {
                propertyTypeMap.put(p, properties.get(p).getPropertyType());
            }
            propertyTypeMap.put("bb", PropertyType.STRING);
            bVertexLabel.ensurePropertiesExist(propertyTypeMap);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalStateException);
            Assert.assertEquals("'B' is a read only foreign VertexLabel!", e.getMessage());
            failed = true;
        }
        Assert.assertTrue(failed);

        this.sqlgGraphFdw.addVertex(T.label, "B.B", "b", "halo");
        this.sqlgGraphFdw.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("B.B").toList();
        Assert.assertEquals(1, vertices.size());

        this.sqlgGraph.addVertex(T.label, "A", "a", "test");
        this.sqlgGraph.tx().commit();
        List<Vertex> aVertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(1, aVertices.size());
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("C", new LinkedHashMap<>() {{
            put("c", PropertyType.STRING);
        }});
        this.sqlgGraph.tx().commit();

        //Assert that one can not insert into a foreign table with sequence primary key.
        failed = false;
        try {
            this.sqlgGraph.addVertex(T.label, "B.B", "b", "halo again");
        } catch (IllegalStateException e) {
            failed = true;
        }
        Assert.assertTrue(failed);
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testImportForeignSchemaVertexAndEdgeLabels() throws SQLException {
        Schema aSchema = this.sqlgGraphFdw.getTopology().ensureSchemaExist("A");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        VertexLabel bVertexLabel = aSchema.ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new HashMap<>() {{
            put("a", PropertyType.STRING);
        }});
        this.sqlgGraphFdw.tx().commit();
        Vertex a = this.sqlgGraphFdw.addVertex(T.label, "A.A", "a", "haloA");
        Vertex b = this.sqlgGraphFdw.addVertex(T.label, "A.B", "a", "haloB");
        a.addEdge("ab", b, "a", "halo ab");
        this.sqlgGraphFdw.tx().commit();
        List<Vertex> vertices = this.sqlgGraphFdw.traversal().V().hasLabel("A.A").out("ab").toList();
        Assert.assertEquals(1, vertices.size());
        this.sqlgGraphFdw.tx().rollback();

        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            String sql = String.format(
                    "CREATE SCHEMA \"%s\";",
                    "A"
            );
            statement.execute(sql);
            sql = String.format(
                    "IMPORT FOREIGN SCHEMA \"%s\" FROM SERVER \"%s\" INTO \"%s\";",
                    "A",
                    "sqlgraph_fwd_server",
                    "A"
            );
            statement.execute(sql);
        }
        this.sqlgGraph.tx().commit();

        Schema a1 = this.sqlgGraphFdw.getTopology().getSchema("A").orElseThrow();
        EdgeLabel ab = a1.getEdgeLabel("ab").orElseThrow();
        this.sqlgGraph.getTopology().importForeignSchemas(Set.of(a1));
        EdgeLabel ab_after =  a1.getEdgeLabel("ab").orElseThrow();
        VertexLabel aVertexLabel_after = a1.getVertexLabel("A").orElseThrow();
        VertexLabel bVertexLabel_after = a1.getVertexLabel("B").orElseThrow();
        Assert.assertSame(ab, ab_after);
        Assert.assertSame(aVertexLabel, aVertexLabel_after);
        Assert.assertSame(bVertexLabel, bVertexLabel_after);

        Optional<Schema> schemaOptional = this.sqlgGraph.getTopology().getSchema("A");
        Assert.assertTrue(schemaOptional.isPresent());
        Assert.assertTrue(schemaOptional.get().isForeignSchema());
        Schema aForeignSchema = schemaOptional.get();
        Assert.assertTrue(aForeignSchema.getVertexLabel("A").isPresent());
        Assert.assertTrue(aForeignSchema.getVertexLabel("A").get().isForeign());
        VertexLabel aForeignVertexLabel = aForeignSchema.getVertexLabel("A").get();
        Assert.assertTrue(aForeignSchema.getVertexLabel("B").isPresent());
        Assert.assertTrue(aForeignSchema.getVertexLabel("B").get().isForeign());
        Assert.assertTrue(aForeignSchema.getEdgeLabel("ab").isPresent());
        Assert.assertTrue(aForeignSchema.getEdgeLabel("ab").get().isForeign());

        vertices = this.sqlgGraph.traversal().V().hasLabel("A.A").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("A.B").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("A.A").out("ab").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("haloB", vertices.get(0).value("a"));

        //Check its readOnly
        boolean failed = false;
        try {
            aForeignSchema.ensureVertexLabelExist("C", new HashMap<>() {{
                put("a", PropertyType.STRING);
            }});
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalStateException);
            Assert.assertEquals("'A' is a read only foreign schema!", e.getMessage());
            failed = true;
        }
        Assert.assertTrue(failed);

        //Add another VertexLabel
        VertexLabel cVertexLabel = aSchema.ensureVertexLabelExist(
                "C",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        aVertexLabel.ensureEdgeLabelExist("ac", cVertexLabel, new HashMap<>() {{
            put("a", PropertyType.STRING);
        }});
        this.sqlgGraphFdw.tx().commit();

        //Check its readOnly
        failed = false;
        try {
            aForeignVertexLabel.ensureEdgeLabelExist("bc", cVertexLabel, new HashMap<>() {{
                put("a", PropertyType.STRING);
            }});
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalStateException);
            Assert.assertEquals("'A' is a read only foreign schema!", e.getMessage());
            failed = true;
        }
        Assert.assertTrue(failed);

        //Check can not insert vertex via foreign label
        failed = false;
        try {
            this.sqlgGraph.addVertex(T.label, "A.A", "a", "haloAgain");
        } catch (IllegalStateException e) {
            failed = true;
        }
        Assert.assertTrue(failed);
        this.sqlgGraph.tx().rollback();

        //Check can not insert edge via foreign label
        failed = false;
        try {
            vertices = this.sqlgGraph.traversal().V().hasLabel("A.A").toList();
            Assert.assertEquals(1, vertices.size());
            a = vertices.get(0);
            vertices = this.sqlgGraph.traversal().V().hasLabel("A.B").toList();
            Assert.assertEquals(1, vertices.size());
            b = vertices.get(0);
            a.addEdge("ab", b);
            this.sqlgGraph.tx().commit();
        } catch (IllegalStateException e) {
            failed = true;
        }
        Assert.assertTrue(failed);
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testVertexLabelWithSameNameInDifferentSchemas() {
        Schema real = this.sqlgGraphFdw.getTopology().ensureSchemaExist("REAL");
        VertexLabel realPerson = real.ensureVertexLabelExist("Person");
        VertexLabel realSoftware = real.ensureVertexLabelExist("Software");
        realPerson.ensureEdgeLabelExist("created", realSoftware);
        Schema plan = this.sqlgGraphFdw.getTopology().ensureSchemaExist("PLAN");
        VertexLabel planPerson = plan.ensureVertexLabelExist("Person");
        VertexLabel planSoftware = plan.ensureVertexLabelExist("Software");
        planPerson.ensureEdgeLabelExist("created", planSoftware);
        this.sqlgGraphFdw.tx().commit();
        this.sqlgGraph.getTopology().importForeignSchemas(Set.of(real, plan));

        Map<String, VertexLabel> vertexLabelMap = real.getVertexLabels();
        Assert.assertTrue(vertexLabelMap.containsKey("REAL.V_Person"));
        Assert.assertTrue(vertexLabelMap.containsKey("REAL.V_Software"));
        VertexLabel vertexLabel = vertexLabelMap.get("REAL.V_Person");
        Map<String, EdgeLabel> edgeLabelMap = vertexLabel.getInEdgeLabels();
        Assert.assertEquals(0, edgeLabelMap.size());
        vertexLabel = vertexLabelMap.get("REAL.V_Software");
        edgeLabelMap = vertexLabel.getInEdgeLabels();
        Assert.assertEquals(1, edgeLabelMap.size());

        Schema foreignReal = this.sqlgGraph.getTopology().getSchema("REAL").orElseThrow();
        vertexLabelMap = foreignReal.getVertexLabels();
        Assert.assertTrue(vertexLabelMap.containsKey("REAL.V_Person"));
        Assert.assertTrue(vertexLabelMap.containsKey("REAL.V_Software"));
        vertexLabel = vertexLabelMap.get("REAL.V_Person");
        edgeLabelMap = vertexLabel.getInEdgeLabels();
        Assert.assertEquals(0, edgeLabelMap.size());
        vertexLabel = vertexLabelMap.get("REAL.V_Software");
        edgeLabelMap = vertexLabel.getInEdgeLabels();
        Assert.assertEquals(1, edgeLabelMap.size());
    }

    @Test
    public void testReimportForeignSchema() {
        Schema aSchema = this.sqlgGraphFdw.getTopology().ensureSchemaExist("A");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        VertexLabel bVertexLabel = aSchema.ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new HashMap<>() {{
            put("a", PropertyType.STRING);
        }});
        this.sqlgGraphFdw.tx().commit();
        this.sqlgGraph.getTopology().importForeignSchemas(Set.of(aSchema));
        this.sqlgGraph.getTopology().clearForeignSchemas();
        this.sqlgGraph.getTopology().importForeignSchemas(Set.of(aSchema));
        Assert.assertTrue(this.sqlgGraph.getTopology().getSchema("A").isPresent());
        Assert.assertTrue(this.sqlgGraph.getTopology().getSchema("A").orElseThrow().getVertexLabel("A").isPresent());
        Assert.assertTrue(this.sqlgGraph.getTopology().getSchema("A").orElseThrow().getVertexLabel("B").isPresent());
    }

    @Test
    public void testImportForeignSchemaEdgeLabelAcrossSchemaInVertexLabelFailure1() throws SQLException {
        Schema aSchema = this.sqlgGraphFdw.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraphFdw.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new HashMap<>() {{
            put("a", PropertyType.STRING);
        }});
        this.sqlgGraphFdw.tx().commit();
        Vertex a = this.sqlgGraphFdw.addVertex(T.label, "A.A", "a", "haloA");
        Vertex b = this.sqlgGraphFdw.addVertex(T.label, "B.B", "a", "haloB");
        a.addEdge("ab", b, "a", "halo ab");
        this.sqlgGraphFdw.tx().commit();
        List<Vertex> vertices = this.sqlgGraphFdw.traversal().V().hasLabel("A.A").out("ab").toList();
        Assert.assertEquals(1, vertices.size());
        this.sqlgGraphFdw.tx().rollback();

        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            String sql = String.format(
                    "CREATE SCHEMA \"%s\";",
                    "A"
            );
            statement.execute(sql);
            sql = String.format(
                    "IMPORT FOREIGN SCHEMA \"%s\" FROM SERVER \"%s\" INTO \"%s\";",
                    "A",
                    "sqlgraph_fwd_server",
                    "A"
            );
            statement.execute(sql);
        }
        this.sqlgGraph.tx().commit();

        boolean failed = false;
        try {
            this.sqlgGraph.getTopology().importForeignSchemas(Set.of(this.sqlgGraphFdw.getTopology().getSchema("A").orElseThrow()));
        } catch (Exception e) {
            failed = true;
            Assert.assertTrue(e instanceof IllegalStateException);
            Assert.assertEquals("EdgeLabel 'A.E_ab' has a inVertexLabel 'B' that is not present in a foreign schema", e.getMessage());
        }
        Assert.assertTrue(failed);
    }

    @Test
    public void testImportForeignSchemaEdgeLabelAcrossSchemaOutVertexLabelFailure1() throws SQLException {
        Schema aSchema = this.sqlgGraphFdw.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraphFdw.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new HashMap<>() {{
            put("a", PropertyType.STRING);
        }});
        this.sqlgGraphFdw.tx().commit();
        Vertex a = this.sqlgGraphFdw.addVertex(T.label, "A.A", "a", "haloA");
        Vertex b = this.sqlgGraphFdw.addVertex(T.label, "B.B", "a", "haloB");
        a.addEdge("ab", b, "a", "halo ab");
        this.sqlgGraphFdw.tx().commit();
        List<Vertex> vertices = this.sqlgGraphFdw.traversal().V().hasLabel("A.A").out("ab").toList();
        Assert.assertEquals(1, vertices.size());
        this.sqlgGraphFdw.tx().rollback();

        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            String sql = String.format(
                    "CREATE SCHEMA \"%s\";",
                    "B"
            );
            statement.execute(sql);
            sql = String.format(
                    "IMPORT FOREIGN SCHEMA \"%s\" FROM SERVER \"%s\" INTO \"%s\";",
                    "B",
                    "sqlgraph_fwd_server",
                    "B"
            );
            statement.execute(sql);
        }
        this.sqlgGraph.tx().commit();

        boolean failed = false;
        try {
            this.sqlgGraph.getTopology().importForeignSchemas(Set.of(this.sqlgGraphFdw.getTopology().getSchema("B").orElseThrow()));
        } catch (Exception e) {
            failed = true;
            Assert.assertTrue(e instanceof IllegalStateException);
            Assert.assertEquals("VertexLabel 'B.V_B' has an inEdgeLabel 'ab' that is not present in a foreign schema", e.getMessage());
        }
        Assert.assertTrue(failed);
    }

    @Test
    public void testImportForeignSchemaEdgeLabelAcrossSchemaOutVertexLabelSuccess() throws SQLException {
        Schema aSchema = this.sqlgGraphFdw.getTopology().ensureSchemaExist("A");
        Schema bSchema = this.sqlgGraphFdw.getTopology().ensureSchemaExist("B");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new HashMap<>() {{
            put("a", PropertyType.STRING);
        }});
        this.sqlgGraphFdw.tx().commit();
        Vertex a = this.sqlgGraphFdw.addVertex(T.label, "A.A", "a", "haloA");
        Vertex b = this.sqlgGraphFdw.addVertex(T.label, "B.B", "a", "haloB");
        a.addEdge("ab", b, "a", "halo ab");
        this.sqlgGraphFdw.tx().commit();
        List<Vertex> vertices = this.sqlgGraphFdw.traversal().V().hasLabel("A.A").out("ab").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraphFdw.traversal().V().hasLabel("B.B").in("ab").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraphFdw.traversal().E().hasLabel("ab").inV().toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("B.B", vertices.get(0).label());
        this.sqlgGraphFdw.tx().rollback();

        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            String sql = String.format(
                    "CREATE SCHEMA \"%s\";",
                    "A"
            );
            statement.execute(sql);
            sql = String.format(
                    "CREATE SCHEMA \"%s\";",
                    "B"
            );
            statement.execute(sql);
            sql = String.format(
                    "IMPORT FOREIGN SCHEMA \"%s\" FROM SERVER \"%s\" INTO \"%s\";",
                    "A",
                    "sqlgraph_fwd_server",
                    "A"
            );
            statement.execute(sql);
            sql = String.format(
                    "IMPORT FOREIGN SCHEMA \"%s\" FROM SERVER \"%s\" INTO \"%s\";",
                    "B",
                    "sqlgraph_fwd_server",
                    "B"
            );
            statement.execute(sql);
        }
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.getTopology().importForeignSchemas(Set.of(
                this.sqlgGraphFdw.getTopology().getSchema("A").orElseThrow(),
                this.sqlgGraphFdw.getTopology().getSchema("B").orElseThrow()
        ));

        vertices = this.sqlgGraph.traversal().V().hasLabel("A.A").out().toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("A.A").out("ab").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("B.B").in("ab").toList();
        Assert.assertEquals(1, vertices.size());
        List<Edge> edges = this.sqlgGraph.traversal().E().hasLabel("ab").toList();
        Assert.assertEquals(1, edges.size());
        vertices = this.sqlgGraph.traversal().E().hasLabel("ab").inV().toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("B.B", vertices.get(0).label());
        edges = this.sqlgGraph.traversal().V().hasLabel("A.A").outE().toList();
        Assert.assertEquals(1, edges.size());
        edges = this.sqlgGraph.traversal().V().hasLabel("B.B").outE().toList();
        Assert.assertEquals(0, edges.size());
        edges = this.sqlgGraph.traversal().V().hasLabel("B.B").inE().toList();
        Assert.assertEquals(1, edges.size());

        this.sqlgGraph.getTopology().clearForeignSchemas();
        Assert.assertTrue(this.sqlgGraph.getTopology().getSchema("A").isEmpty());
        Assert.assertTrue(this.sqlgGraph.getTopology().getSchema("B").isEmpty());
    }

    @Test
    public void testImportForeignSchemaVertexAndEdgeLabelInPublic() throws SQLException {
        VertexLabel aVertexLabel = this.sqlgGraphFdw.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        VertexLabel bVertexLabel = this.sqlgGraphFdw.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        @SuppressWarnings("unused")
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new HashMap<>() {{
            put("a", PropertyType.STRING);
        }});
        Vertex a = this.sqlgGraphFdw.addVertex(T.label, "A", "a", "halo A");
        Vertex b = this.sqlgGraphFdw.addVertex(T.label, "B", "a", "halo B");
        a.addEdge("ab", b, "a", "halo ab edge");
        this.sqlgGraphFdw.tx().commit();

        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            String sql = String.format(
                    "IMPORT FOREIGN SCHEMA \"%s\" LIMIT TO (%s) FROM SERVER \"%s\" INTO \"%s\";",
                    this.sqlgGraph.getSqlDialect().getPublicSchema(),
                    "\"V_A\",\"V_B\",\"E_ab\"",
                    "sqlgraph_fwd_server",
                    this.sqlgGraph.getSqlDialect().getPublicSchema()
            );
            statement.execute(sql);
        }
        this.sqlgGraph.tx().commit();

        VertexLabel foreignAVertexLabel = this.sqlgGraphFdw.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow();
        VertexLabel foreignBVertexLabel = this.sqlgGraphFdw.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow();
        EdgeLabel foreignABEdgeLabel = this.sqlgGraphFdw.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
        this.sqlgGraph.getTopology().importForeignVertexEdgeLabels(
                this.sqlgGraph.getTopology().getPublicSchema(),
                Set.of(foreignAVertexLabel, foreignBVertexLabel),
                Set.of(foreignABEdgeLabel)
        );

        Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").isPresent());
        Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").isPresent());
        Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").isPresent());

        List<Vertex> aVertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(1, aVertices.size());
        List<Vertex> bVertices = this.sqlgGraph.traversal().V().hasLabel("B").toList();
        Assert.assertEquals(1, bVertices.size());
        List<Edge> abEdges = this.sqlgGraph.traversal().E().hasLabel("ab").toList();
        Assert.assertEquals(1, abEdges.size());
        bVertices = this.sqlgGraph.traversal().V().hasLabel("A").out().toList();
        Assert.assertEquals(1, bVertices.size());

        this.sqlgGraph.getTopology().clearForeignSchemas();
        Assert.assertFalse(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").isPresent());
        Assert.assertFalse(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").isPresent());
        Assert.assertFalse(this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").isPresent());
    }

    @Test
    public void testImportForeignSchemaVertexAndEdgeLabelInPublicFailure() throws SQLException {
        VertexLabel aVertexLabel = this.sqlgGraphFdw.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        VertexLabel bVertexLabel = this.sqlgGraphFdw.getTopology().getPublicSchema().ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        @SuppressWarnings("unused")
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new HashMap<>() {{
            put("a", PropertyType.STRING);
        }});
        @SuppressWarnings("unused")
        EdgeLabel baEdgeLabel = bVertexLabel.ensureEdgeLabelExist("ba", aVertexLabel, new HashMap<>() {{
            put("a", PropertyType.STRING);
        }});
        Vertex a = this.sqlgGraphFdw.addVertex(T.label, "A", "a", "halo A");
        Vertex b = this.sqlgGraphFdw.addVertex(T.label, "B", "a", "halo B");
        a.addEdge("ab", b, "a", "halo ab edge");
        b.addEdge("ba", a, "a", "halo ba edge");
        this.sqlgGraphFdw.tx().commit();

        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            String sql = String.format(
                    "IMPORT FOREIGN SCHEMA \"%s\" LIMIT TO (%s) FROM SERVER \"%s\" INTO \"%s\";",
                    this.sqlgGraph.getSqlDialect().getPublicSchema(),
                    "\"V_A\",\"V_B\",\"E_ab\",\"E_ba\"",
                    "sqlgraph_fwd_server",
                    this.sqlgGraph.getSqlDialect().getPublicSchema()
            );
            statement.execute(sql);
        }
        this.sqlgGraph.tx().commit();

        VertexLabel foreignAVertexLabel = this.sqlgGraphFdw.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow();
        VertexLabel foreignBVertexLabel = this.sqlgGraphFdw.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow();
        EdgeLabel foreignABEdgeLabel = this.sqlgGraphFdw.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
        EdgeLabel foreignBAEdgeLabel = this.sqlgGraphFdw.getTopology().getPublicSchema().getEdgeLabel("ba").orElseThrow();

        boolean failure = false;
        try {
            this.sqlgGraph.getTopology().importForeignVertexEdgeLabels(
                    this.sqlgGraph.getTopology().getPublicSchema(),
                    Set.of(foreignAVertexLabel, foreignBVertexLabel),
                    Set.of(foreignABEdgeLabel)
            );
        } catch (Exception e) {
            failure = true;
            Assert.assertTrue(e instanceof IllegalStateException);
            Assert.assertEquals("'public.E_public.ba' is not present in the foreign EdgeLabels", e.getMessage());
        }
        Assert.assertTrue(failure);

        this.sqlgGraph.getTopology().importForeignVertexEdgeLabels(
                this.sqlgGraph.getTopology().getPublicSchema(),
                Set.of(foreignAVertexLabel, foreignBVertexLabel),
                Set.of(foreignABEdgeLabel, foreignBAEdgeLabel)
        );

        Optional<VertexLabel> vertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        Assert.assertTrue(vertexLabelOptional.isPresent());
        Assert.assertTrue(vertexLabelOptional.get().isForeign());

        vertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B");
        Assert.assertTrue(vertexLabelOptional.isPresent());
        Assert.assertTrue(vertexLabelOptional.get().isForeign());

        Optional<EdgeLabel> edgeLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab");
        Assert.assertTrue(edgeLabelOptional.isPresent());
        Assert.assertTrue(edgeLabelOptional.get().isForeign());

        edgeLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ba");
        Assert.assertTrue(edgeLabelOptional.isPresent());
        Assert.assertTrue(edgeLabelOptional.get().isForeign());

        List<Vertex> aVertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(1, aVertices.size());
        List<Vertex> bVertices = this.sqlgGraph.traversal().V().hasLabel("B").toList();
        Assert.assertEquals(1, bVertices.size());
        List<Edge> abEdges = this.sqlgGraph.traversal().E().hasLabel("ab").toList();
        Assert.assertEquals(1, abEdges.size());
        List<Edge> baEdges = this.sqlgGraph.traversal().E().hasLabel("ba").toList();
        Assert.assertEquals(1, baEdges.size());
        bVertices = this.sqlgGraph.traversal().V().hasLabel("A").out().toList();
        Assert.assertEquals(1, bVertices.size());
        bVertices = this.sqlgGraph.traversal().V().hasLabel("A").in().toList();
        Assert.assertEquals(1, bVertices.size());
    }

    @Test
    public void importTinkerPopClassic() throws SQLException {
        loadModern(this.sqlgGraphFdw);
        this.sqlgGraphFdw.tx().commit();
        VertexLabel person = this.sqlgGraphFdw.getTopology().getPublicSchema().getVertexLabel("person").orElseThrow();
        VertexLabel software = this.sqlgGraphFdw.getTopology().getPublicSchema().getVertexLabel("software").orElseThrow();
        EdgeLabel created = this.sqlgGraphFdw.getTopology().getPublicSchema().getEdgeLabel("created").orElseThrow();
        EdgeLabel knows = this.sqlgGraphFdw.getTopology().getPublicSchema().getEdgeLabel("knows").orElseThrow();

        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            String sql = String.format(
                    "IMPORT FOREIGN SCHEMA \"%s\" LIMIT TO (%s) FROM SERVER \"%s\" INTO \"%s\";",
                    this.sqlgGraph.getSqlDialect().getPublicSchema(),
                    "\"V_person\",\"V_software\",\"E_created\",\"E_knows\"",
                    "sqlgraph_fwd_server",
                    this.sqlgGraph.getSqlDialect().getPublicSchema()
            );
            statement.execute(sql);
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().importForeignVertexEdgeLabels(
                this.sqlgGraph.getTopology().getPublicSchema(),
                Set.of(person, software),
                Set.of(created, knows)
        );
        final Traversal<Vertex, Map<String, Map<String, Map<String, Object>>>> traversal = this.sqlgGraph.traversal()
                .V().hasLabel("person")
                .filter(__.outE("created")).aggregate("p").as("p1").values("name").as("p1n")
                .select("p").unfold().where(P.neq("p1")).as("p2").values("name").as("p2n").select("p2")
                .out("created").choose(__.in("created").where(P.eq("p1")), __.values("name"), __.constant(Collections.emptyList()))
                .<String, Map<String, Map<String, Object>>>group().by(__.select("p1n")).
                by(__.group().by(__.select("p2n")).
                        by(__.unfold().fold().project("numCoCreated", "coCreated").by(__.count(Scope.local)).by()));
        this.printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        checkCoworkerSummary(traversal.next());
        Assert.assertFalse(traversal.hasNext());
    }

    private static void checkCoworkerSummary(final Map<String, Map<String, Map<String, Object>>> summary) {
        Assert.assertNotNull(summary);
        Assert.assertEquals(3, summary.size());
        Assert.assertTrue(summary.containsKey("marko"));
        Assert.assertTrue(summary.containsKey("josh"));
        Assert.assertTrue(summary.containsKey("peter"));
        for (final Map.Entry<String, Map<String, Map<String, Object>>> entry : summary.entrySet()) {
            assertEquals(2, entry.getValue().size());
            switch (entry.getKey()) {
                case "marko":
                    Assert.assertTrue(entry.getValue().containsKey("josh") && entry.getValue().containsKey("peter"));
                    break;
                case "josh":
                    Assert.assertTrue(entry.getValue().containsKey("peter") && entry.getValue().containsKey("marko"));
                    break;
                case "peter":
                    Assert.assertTrue(entry.getValue().containsKey("marko") && entry.getValue().containsKey("josh"));
                    break;
            }
            for (final Map<String, Object> m : entry.getValue().values()) {
                Assert.assertTrue(m.containsKey("numCoCreated"));
                Assert.assertTrue(m.containsKey("coCreated"));
                Assert.assertTrue(m.get("numCoCreated") instanceof Number);
                Assert.assertTrue(m.get("coCreated") instanceof Collection);
                Assert.assertEquals(1, ((Number) m.get("numCoCreated")).intValue());
                Assert.assertEquals(1, ((Collection) m.get("coCreated")).size());
                Assert.assertEquals("lop", ((Collection) m.get("coCreated")).iterator().next());
            }
        }
    }

    @Test
    public void importTinkerPopGratefulDead() throws SQLException {
        loadGratefulDead(this.sqlgGraphFdw);
        this.sqlgGraphFdw.tx().commit();
        VertexLabel song = this.sqlgGraphFdw.getTopology().getPublicSchema().getVertexLabel("song").orElseThrow();
        VertexLabel artist = this.sqlgGraphFdw.getTopology().getPublicSchema().getVertexLabel("artist").orElseThrow();
        EdgeLabel followedBy = this.sqlgGraphFdw.getTopology().getPublicSchema().getEdgeLabel("followedBy").orElseThrow();
        EdgeLabel writtenBy = this.sqlgGraphFdw.getTopology().getPublicSchema().getEdgeLabel("writtenBy").orElseThrow();
        EdgeLabel sungBy = this.sqlgGraphFdw.getTopology().getPublicSchema().getEdgeLabel("sungBy").orElseThrow();

        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            String sql = String.format(
                    "IMPORT FOREIGN SCHEMA \"%s\" LIMIT TO (%s) FROM SERVER \"%s\" INTO \"%s\";",
                    this.sqlgGraph.getSqlDialect().getPublicSchema(),
                    "\"V_song\",\"V_artist\",\"E_followedBy\",\"E_writtenBy\", \"E_sungBy\"",
                    "sqlgraph_fwd_server",
                    this.sqlgGraph.getSqlDialect().getPublicSchema()
            );
            statement.execute(sql);
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().importForeignVertexEdgeLabels(
                this.sqlgGraph.getTopology().getPublicSchema(),
                Set.of(song, artist),
                Set.of(followedBy, writtenBy, sungBy)
        );
        final Traversal<Vertex, Map<String, List<String>>> traversal = getPlaylistPaths();
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        Map<String, List<String>> map = traversal.next();
        Assert.assertTrue(map.get("artists").contains("Bob_Dylan"));
        boolean hasJohnnyCash = false;
        while (traversal.hasNext()) {
            map = traversal.next();
            if (map.get("artists").contains("Johnny_Cash"))
                hasJohnnyCash = true;
        }
        Assert.assertTrue(hasJohnnyCash);
        Assert.assertTrue(map.get("artists").contains("Grateful_Dead"));
    }

    @SuppressWarnings("unchecked")
    private Traversal<Vertex, Map<String, List<String>>> getPlaylistPaths() {
        return this.sqlgGraph.traversal().V().has("name", "Bob_Dylan").in("sungBy").as("a").
                repeat(__.out().order().by(Order.shuffle).simplePath().from("a")).
                until(__.out("writtenBy").has("name", "Johnny_Cash")).limit(1).as("b").
                repeat(__.out().order().by(Order.shuffle).as("c").simplePath().from("b").to("c")).
                until(__.out("sungBy").has("name", "Grateful_Dead")).limit(1).
                path().from("a").unfold().
                        <List<String>>project("song", "artists").
                by("name").
                by(__.coalesce(__.out("sungBy", "writtenBy").dedup().values("name"), __.constant("Unknown")).fold());
    }

    @Test
    public void testQueryViaFDW_WhileInsertingDirectly() throws SQLException, InterruptedException {
        this.sqlgGraphFdw.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("a", PropertyType.STRING);
                }}
        );
        this.sqlgGraphFdw.tx().commit();

        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            String sql = String.format(
                    "IMPORT FOREIGN SCHEMA \"%s\" LIMIT TO (%s) FROM SERVER \"%s\" INTO \"%s\";",
                    this.sqlgGraph.getSqlDialect().getPublicSchema(),
                    "\"V_A\"",
                    "sqlgraph_fwd_server",
                    this.sqlgGraph.getSqlDialect().getPublicSchema()
            );
            statement.execute(sql);
        }
        this.sqlgGraph.tx().commit();

        ExecutorService executorService1 = Executors.newFixedThreadPool(1);
        executorService1.submit(() -> {
            for (int i = 0; i < 10_000; i++) {
                this.sqlgGraphFdw.addVertex(T.label, "A", "a", "halo A");
            }
            this.sqlgGraphFdw.tx().commit();
            LOGGER.info("Completed executorService1");
        });
        ExecutorService executorService2 = Executors.newFixedThreadPool(1);
        executorService2.submit(() -> {
            for (int i = 0; i < 10_000; i++) {
                List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
                Assert.assertEquals(0, vertices.size());
            }
            this.sqlgGraphFdw.tx().rollback();
            LOGGER.info("Completed executorService2");
        });
        executorService1.shutdown();
        if (!executorService1.awaitTermination(1, TimeUnit.MINUTES)) {
            Assert.fail("ExecutorService did not shutdown in 1 minute");
        }
        executorService2.shutdown();
        if (!executorService2.awaitTermination(1, TimeUnit.MINUTES)) {
            Assert.fail("ExecutorService did not shutdown in 1 minute");
        }
        LOGGER.debug("Done");
    }

    @Test
    public void testInsertViaForeignSchema() throws SQLException {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uuid", PropertyType.UUID);
                    put("a", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Set.of("uuid"))
        );
        this.sqlgGraph.tx().commit();

        Schema b = this.sqlgGraphFdw.getTopology().ensureSchemaExist("B");
        @SuppressWarnings("UnusedAssignment")
        VertexLabel bVertexLabel = b.ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uuid", PropertyType.UUID);
                    put("b", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Set.of("uuid"))
        );
        this.sqlgGraphFdw.tx().commit();

        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            String sql = String.format(
                    "CREATE SCHEMA \"%s\";",
                    "B"
            );
            statement.execute(sql);
            sql = String.format(
                    "IMPORT FOREIGN SCHEMA \"%s\" FROM SERVER \"%s\" INTO \"%s\";",
                    "B",
                    "sqlgraph_fwd_server",
                    "B"
            );
            statement.execute(sql);
        }
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.getTopology().importForeignSchemas(Set.of(this.sqlgGraphFdw.getTopology().getSchema("B").orElseThrow()));
        Assert.assertTrue(this.sqlgGraph.getTopology().getSchema("B").isPresent());

        //Check its readOnly
        Schema foreignSchema = this.sqlgGraph.getTopology().getSchema("B").orElseThrow();
        boolean failed = false;
        try {
            foreignSchema.ensureVertexLabelExist("D",
                    new LinkedHashMap<>() {{
                        put("d", PropertyType.STRING);
                    }});
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalStateException);
            Assert.assertEquals("'B' is a read only foreign schema!", e.getMessage());
            failed = true;
        }
        Assert.assertTrue(failed);
        failed = false;
        try {
            bVertexLabel = foreignSchema.getVertexLabel("B").orElseThrow();
            Map<String, PropertyColumn> properties = bVertexLabel.getProperties();
            Map<String, PropertyType> propertyTypeMap = new HashMap<>();
            for (String p : properties.keySet()) {
                propertyTypeMap.put(p, properties.get(p).getPropertyType());
            }
            propertyTypeMap.put("bb", PropertyType.STRING);
            bVertexLabel.ensurePropertiesExist(propertyTypeMap);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalStateException);
            Assert.assertEquals("'B' is a read only foreign VertexLabel!", e.getMessage());
            failed = true;
        }
        Assert.assertTrue(failed);

        this.sqlgGraphFdw.addVertex(T.label, "B.B", "uuid", UUID.randomUUID(), "b", "halo");
        this.sqlgGraphFdw.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("B.B").toList();
        Assert.assertEquals(1, vertices.size());

        this.sqlgGraph.addVertex(T.label, "A", "uuid", UUID.randomUUID(), "a", "test");
        this.sqlgGraph.tx().commit();
        List<Vertex> aVertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(1, aVertices.size());
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("C", new LinkedHashMap<>() {{
            put("c", PropertyType.STRING);
        }});
        this.sqlgGraph.tx().commit();

        //Assert that one can not insert into a foreign table with sequence primary key.
        failed = false;
        try {
            this.sqlgGraph.addVertex(T.label, "B.B", "uuid", UUID.randomUUID(), "b", "halo again");
        } catch (IllegalStateException e) {
            failed = true;
        }
        Assert.assertFalse(failed);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraphFdw.traversal().V().hasLabel("B.B").count().next(), 0L);
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("B.B").count().next(), 0L);
        this.sqlgGraph.tx().rollback();
        this.sqlgGraphFdw.tx().rollback();
    }

    @Test
    public void testInsertEdgesViaForeignSchema() throws SQLException {
        Schema a = this.sqlgGraphFdw.getTopology().ensureSchemaExist("A");
        VertexLabel aVertexLabel = a.ensureVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uuid", PropertyType.UUID);
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Set.of("uuid"))
        );
        VertexLabel bVertexLabel = a.ensureVertexLabelExist(
                "B",
                new LinkedHashMap<>() {{
                    put("uuid", PropertyType.UUID);
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Set.of("uuid"))
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new HashMap<>() {{
                    put("uuid", PropertyType.UUID);
                }},
                ListOrderedSet.listOrderedSet(Set.of("uuid"))
        );
        this.sqlgGraphFdw.tx().commit();

        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            String sql = String.format(
                    "CREATE SCHEMA \"%s\";",
                    "A"
            );
            statement.execute(sql);
            sql = String.format(
                    "IMPORT FOREIGN SCHEMA \"%s\" FROM SERVER \"%s\" INTO \"%s\";",
                    "A",
                    "sqlgraph_fwd_server",
                    "A"
            );
            statement.execute(sql);
        }
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.getTopology().importForeignSchemas(Set.of(this.sqlgGraphFdw.getTopology().getSchema("A").orElseThrow()));
        Assert.assertTrue(this.sqlgGraph.getTopology().getSchema("A").isPresent());

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A", "uuid", UUID.randomUUID(), "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A.A", "uuid", UUID.randomUUID(), "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "A.B", "uuid", UUID.randomUUID(), "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "A.B", "uuid", UUID.randomUUID(), "name", "b2");
        a1.addEdge("ab", b1, "uuid", UUID.randomUUID());
        a1.addEdge("ab", b2, "uuid", UUID.randomUUID());
        a2.addEdge("ab", b1, "uuid", UUID.randomUUID());
        a2.addEdge("ab", b2, "uuid", UUID.randomUUID());
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraphFdw.traversal().V().hasLabel("A.A").count().next(), 0L);
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("A.A").count().next(), 0L);
        Assert.assertEquals(2, this.sqlgGraphFdw.traversal().V().hasLabel("A.B").count().next(), 0L);
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("A.B").count().next(), 0L);
        Assert.assertEquals(4, this.sqlgGraphFdw.traversal().E().hasLabel("A.ab").count().next(), 0L);
        Assert.assertEquals(4, this.sqlgGraph.traversal().E().hasLabel("A.ab").count().next(), 0L);

        Assert.assertEquals(2, this.sqlgGraphFdw.traversal().V(a1.id()).out("ab").count().next(), 0L);
        Assert.assertEquals(2, this.sqlgGraphFdw.traversal().V(a2.id()).out("ab").count().next(), 0L);
        Assert.assertEquals(2, this.sqlgGraphFdw.traversal().V(b1.id()).in("ab").count().next(), 0L);
        Assert.assertEquals(2, this.sqlgGraphFdw.traversal().V(b2.id()).in("ab").count().next(), 0L);
        this.sqlgGraph.tx().rollback();
        this.sqlgGraphFdw.tx().rollback();
    }
}
