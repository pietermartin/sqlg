package org.umlg.sqlg.test;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Date: 2014/07/12
 * Time: 5:44 PM
 */
public abstract class BaseTest {

    private static Logger logger = LoggerFactory.getLogger(BaseTest.class.getName());
    protected SqlgGraph sqlgGraph;
    protected GraphTraversalSource gt;
    protected static Configuration configuration;

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            logger.info("Starting test: " + description.getClassName() + "." + description.getMethodName());
        }

        protected void finished(Description description) {
            logger.info("Finished test: " + description.getClassName() + "." + description.getMethodName());
        }
    };

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
            if (!configuration.containsKey("jdbc.url")) {
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
            }
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void before() throws Exception {
        this.sqlgGraph = SqlgGraph.open(configuration);
        SqlgUtil.dropDb(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        this.gt = this.sqlgGraph.traversal();
    }

    @After
    public void after() throws Exception {
        this.sqlgGraph.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);
        this.sqlgGraph.close();
    }

    protected GraphTraversal<Vertex, Vertex> vertexTraversal(Vertex v) {
        return v.graph().traversal().V(v);
    }

    protected GraphTraversal<Edge, Edge> edgeTraversal(Edge e) {
        return e.graph().traversal().E(e.id());
    }

    protected void assertDb(String table, int numberOfRows) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = this.sqlgGraph.getSqlgDataSource().get(this.sqlgGraph.getJdbcUrl()).getConnection();
            stmt = conn.createStatement();
            StringBuilder sql = new StringBuilder("SELECT * FROM ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            ResultSet rs = stmt.executeQuery(sql.toString());
            int countRows = 0;
            while (rs.next()) {
                countRows++;
            }
            assertEquals(numberOfRows, countRows);
            rs.close();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            //noinspection EmptyCatchBlock
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                fail(se.getMessage());
            }
        }

    }

    protected void printTraversalForm(final Traversal traversal) {
        final boolean muted = Boolean.parseBoolean(System.getProperty("muteTestLogs", "false"));

        if (!muted) System.out.println("   pre-strategy:" + traversal);
        traversal.hasNext();
        if (!muted) System.out.println("  post-strategy:" + traversal);
    }

    protected void loadModern(SqlgGraph sqlgGraph) {
        final GraphReader gryoReader = GryoReader.build().create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/org/apache/tinkerpop/gremlin/structure/io/gryo/tinkerpop-modern.kryo")) {
            gryoReader.readGraph(stream, sqlgGraph);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    protected void loadModern() {
        loadModern(this.sqlgGraph);
    }

    /**
     * Looks up the identifier as generated by the current source graph being tested.
     *
     * @param vertexName a unique string that will identify a graph element within a graph
     * @return the id as generated by the graph
     */
    public Object convertToVertexId(final String vertexName) {
        return convertToVertexId(this.sqlgGraph, vertexName);
    }

    /**
     * Looks up the identifier as generated by the current source graph being tested.
     *
     * @param g          the graph to get the element id from
     * @param vertexName a unique string that will identify a graph element within a graph
     * @return the id as generated by the graph
     */
    public Object convertToVertexId(final Graph g, final String vertexName) {
        return convertToVertex(g, vertexName).id();
    }

    protected Vertex convertToVertex(final Graph graph, final String vertexName) {
        // all test graphs have "name" as a unique id which makes it easy to hardcode this...works for now
        return graph.traversal().V().has("name", vertexName).next();
    }

    public Object convertToEdgeId(final Graph graph, final String outVertexName, String edgeLabel, final String inVertexName) {
        return graph.traversal().V().has("name", outVertexName).outE(edgeLabel).as("e").inV().has("name", inVertexName).<Edge>select("e").next().id();
    }

    public static void assertModernGraph(final Graph g1, final boolean assertDouble, final boolean lossyForId) {
        assertToyGraph(g1, assertDouble, lossyForId, true);
    }

    private static void assertToyGraph(final Graph g1, final boolean assertDouble, final boolean lossyForId, final boolean assertSpecificLabel) {
        assertEquals(new Long(6), g1.traversal().V().count().next());
        assertEquals(new Long(6), g1.traversal().E().count().next());

        final Vertex v1 = g1.traversal().V().has("name", "marko").next();
        assertEquals(29, v1.<Integer>value("age").intValue());
        assertEquals(2, v1.keys().size());
        assertEquals(assertSpecificLabel ? "person" : Vertex.DEFAULT_LABEL, v1.label());
        assertId(g1, lossyForId, v1, 1);

        final List<Edge> v1Edges = g1.traversal().V(v1.id()).bothE().toList();
        assertEquals(3, v1Edges.size());
        v1Edges.forEach(e -> {
            if (g1.traversal().E(e.id()).inV().values("name").next().equals("vadas")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertEquals(0.5d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.5f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 7);
            } else if (g1.traversal().E(e.id()).inV().values("name").next().equals("josh")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertEquals(1.0, e.value("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 8);
            } else if (g1.traversal().E(e.id()).inV().values("name").next().equals("lop")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(0.4d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 9);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v2 = g1.traversal().V().has("name", "vadas").next();
        assertEquals(27, v2.<Integer>value("age").intValue());
        assertEquals(2, v2.keys().size());
        assertEquals(assertSpecificLabel ? "person" : Vertex.DEFAULT_LABEL, v2.label());
        assertId(g1, lossyForId, v2, 2);

        final List<Edge> v2Edges = g1.traversal().V(v2.id()).bothE().toList();
        assertEquals(1, v2Edges.size());
        v2Edges.forEach(e -> {
            if (g1.traversal().E(e.id()).outV().values("name").next().equals("marko")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertEquals(0.5d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.5f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 7);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v3 = g1.traversal().V().has("name", "lop").next();
        assertEquals("java", v3.<String>value("lang"));
        assertEquals(2, v2.keys().size());
        assertEquals(assertSpecificLabel ? "software" : Vertex.DEFAULT_LABEL, v3.label());
        assertId(g1, lossyForId, v3, 3);

        final List<Edge> v3Edges = g1.traversal().V(v3.id()).bothE().toList();
        assertEquals(3, v3Edges.size());
        v3Edges.forEach(e -> {
            if (g1.traversal().E(e.id()).outV().values("name").next().equals("peter")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(0.2d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.2f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 12);
            } else if (g1.traversal().E(e.id()).outV().next().value("name").equals("josh")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(0.4d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 11);
            } else if (g1.traversal().E(e.id()).outV().values("name").next().equals("marko")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(0.4d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 9);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v4 = g1.traversal().V().has("name", "josh").next();
        assertEquals(32, v4.<Integer>value("age").intValue());
        assertEquals(2, v4.keys().size());
        assertEquals(assertSpecificLabel ? "person" : Vertex.DEFAULT_LABEL, v4.label());
        assertId(g1, lossyForId, v4, 4);

        final List<Edge> v4Edges = g1.traversal().V(v4.id()).bothE().toList();
        assertEquals(3, v4Edges.size());
        v4Edges.forEach(e -> {
            if (e.inVertex().values("name").next().equals("ripple")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(1.0d, e.value("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 10);
            } else if (e.inVertex().values("name").next().equals("lop")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(0.4d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 11);
            } else if (e.outVertex().values("name").next().equals("marko")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertEquals(1.0d, e.value("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 8);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v5 = g1.traversal().V().has("name", "ripple").next();
        assertEquals("java", v5.<String>value("lang"));
        assertEquals(2, v5.keys().size());
        assertEquals(assertSpecificLabel ? "software" : Vertex.DEFAULT_LABEL, v5.label());
        assertId(g1, lossyForId, v5, 5);

        final List<Edge> v5Edges = IteratorUtils.list(v5.edges(Direction.BOTH));
        assertEquals(1, v5Edges.size());
        v5Edges.forEach(e -> {
            if (e.outVertex().values("name").next().equals("josh")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(1.0d, e.value("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 10);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v6 = g1.traversal().V().has("name", "peter").next();
        assertEquals(35, v6.<Integer>value("age").intValue());
        assertEquals(2, v6.keys().size());
        assertEquals(assertSpecificLabel ? "person" : Vertex.DEFAULT_LABEL, v6.label());
        assertId(g1, lossyForId, v6, 6);

        final List<Edge> v6Edges = IteratorUtils.list(v6.edges(Direction.BOTH));
        assertEquals(1, v6Edges.size());
        v6Edges.forEach(e -> {
            if (e.inVertex().values("name").next().equals("lop")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(0.2d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.2f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 12);
            } else {
                fail("Edge not expected");
            }
        });
    }

    private static void assertId(final Graph g, final boolean lossyForId, final Element e, final Object expected) {
        if (g.features().edge().supportsUserSuppliedIds()) {
            if (lossyForId)
                assertEquals(expected.toString(), e.id().toString());
            else
                assertEquals(expected, e.id());
        }
    }
}
