package org.umlg.sqlg.test.topology;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.IndexType;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Date: 2017/01/15
 * Time: 4:30 PM
 */
public class TestValidateTopology extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            configuration.addProperty("validate.topology", true);
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSchemaDoesNotExist() {
        this.sqlgGraph.addVertex(T.label, "A.A");
        this.sqlgGraph.tx().commit();
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            if (this.sqlgGraph.getSqlDialect().needsSchemaDropCascade()) {
                statement.execute("DROP SCHEMA " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") + " CASCADE");
            } else {
                if (this.sqlgGraph.getSqlDialect().isMssqlServer()) {
                    statement.execute("DROP TABLE " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") +
                            "." +
                            this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("V_A"));
                    statement.execute("DROP SCHEMA " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A"));
                } else {
                    statement.execute("DROP SCHEMA " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A"));
                }
            }
            this.sqlgGraph.tx().commit();
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            assertEquals(1, sqlgGraph1.getTopology().getValidationErrors().size());
        }
    }

    @Test
    public void testVertexLabelDoesNotExist() {
        this.sqlgGraph.addVertex(T.label, "A.A");
        this.sqlgGraph.tx().commit();
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute("DROP TABLE " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") + "." +
                    this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("V_A") +
                    (this.sqlgGraph.getSqlDialect().supportsCascade() ? " CASCADE " : ""));
            this.sqlgGraph.tx().commit();
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            assertEquals(1, sqlgGraph1.getTopology().getValidationErrors().size());
        }
    }

    @Test
    public void testEdgeLabelDoesNotExist() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B.B");
        a.addEdge("ab", b);
        this.sqlgGraph.tx().commit();
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute("DROP TABLE " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") + "." +
                    this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("E_ab") +
                    (this.sqlgGraph.getSqlDialect().supportsCascade() ? " CASCADE ": ""));
            this.sqlgGraph.tx().commit();
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            assertEquals(1, sqlgGraph1.getTopology().getValidationErrors().size());
        }
    }

    @Test
    public void testVertexLabelPropertyDoesNotExist() {
        this.sqlgGraph.addVertex(T.label, "A.A", "name", "aaa");
        this.sqlgGraph.tx().commit();
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            if (!this.sqlgGraph.getSqlDialect().isMssqlServer()) {
                statement.execute("ALTER TABLE " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("V_A") + " DROP " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("name"));
            } else {
                statement.execute("ALTER TABLE " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("V_A") + " DROP COLUMN " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("name"));
            }
            this.sqlgGraph.tx().commit();
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            assertEquals(1, sqlgGraph1.getTopology().getValidationErrors().size());
        }
    }

    @Test
    public void testEdgeLabelPropertyDoesNotExist() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A", "name", "aaa");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B.B", "name", "bbb");
        a.addEdge("ab", b, "name", "asdadasdasd");
        this.sqlgGraph.tx().commit();
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            if (!this.sqlgGraph.getSqlDialect().isMssqlServer()) {
                statement.execute("ALTER TABLE " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("E_ab") + " DROP " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("name"));
            } else {
                statement.execute("ALTER TABLE " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("E_ab") + " DROP COLUMN " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("name"));
            }
            this.sqlgGraph.tx().commit();
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            assertEquals(1, sqlgGraph1.getTopology().getValidationErrors().size());
        }
    }

    @Test
    public void testIndexDoesNotExist() {
        this.sqlgGraph.addVertex(T.label, "A.A", "name", "aaa");
        List<PropertyColumn> properties = new ArrayList<>(this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("A").get().getProperties().values());
        this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("A").get().ensureIndexExists(IndexType.UNIQUE, properties);
        this.sqlgGraph.tx().commit();

        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            String indexName = this.sqlgGraph.getSqlDialect().indexName(SchemaTable.of("A", "A"), Topology.VERTEX_PREFIX, Collections.singletonList("name"));
            if (this.sqlgGraph.getSqlDialect().isMssqlServer()) {
                statement.execute("DROP INDEX " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("V_A") + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(indexName));
            } else if (this.sqlgGraph.getSqlDialect().isMariaDb()) {
                statement.execute("DROP INDEX " +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(indexName) +
                        " ON " +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("V_A"));
            } else {
                statement.execute("DROP INDEX " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(indexName));

            }
            this.sqlgGraph.tx().commit();
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            assertEquals(1, sqlgGraph1.getTopology().getValidationErrors().size());
        }
    }

}
