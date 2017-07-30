package org.umlg.sqlg.test.topology;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
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
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
            configuration.addProperty("validate.topology", true);
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

//    @Test
//    public void testSchemaDoesNotExist() throws Exception {
//        this.sqlgGraph.addVertex(T.label, "A.A");
//        this.sqlgGraph.tx().commit();
//        Connection conn = this.sqlgGraph.tx().getConnection();
//        try (Statement statement = conn.createStatement()) {
//            if (this.sqlgGraph.getSqlDialect().needsSchemaDropCascade()) {
//                statement.execute("DROP SCHEMA " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") + " CASCADE");
//            } else {
//                statement.execute("DROP SCHEMA " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A"));
//            }
//            this.sqlgGraph.tx().commit();
//        } catch (SQLException e) {
//            fail(e.getMessage());
//        }
//        this.sqlgGraph.close();
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            assertEquals(1, sqlgGraph1.getTopology().getValidationErrors().size());
//        }
//    }
//
//    @Test
//    public void testVertexLabelDoesNotExist() throws Exception {
//        this.sqlgGraph.addVertex(T.label, "A.A");
//        this.sqlgGraph.tx().commit();
//        Connection conn = this.sqlgGraph.tx().getConnection();
//        try (Statement statement = conn.createStatement()) {
//            statement.execute("DROP TABLE " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") + "." +
//                    this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("V_A") + " CASCADE ");
//            this.sqlgGraph.tx().commit();
//        } catch (SQLException e) {
//            fail(e.getMessage());
//        }
//        this.sqlgGraph.close();
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            assertEquals(1, sqlgGraph1.getTopology().getValidationErrors().size());
//        }
//    }
//
//    @Test
//    public void testEdgeLabelDoesNotExist() throws Exception {
//        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A");
//        Vertex b = this.sqlgGraph.addVertex(T.label, "B.B");
//        a.addEdge("ab", b);
//        this.sqlgGraph.tx().commit();
//        Connection conn = this.sqlgGraph.tx().getConnection();
//        try (Statement statement = conn.createStatement()) {
//            statement.execute("DROP TABLE " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") + "." +
//                    this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("E_ab") + " CASCADE ");
//            this.sqlgGraph.tx().commit();
//        } catch (SQLException e) {
//            fail(e.getMessage());
//        }
//        this.sqlgGraph.close();
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            assertEquals(1, sqlgGraph1.getTopology().getValidationErrors().size());
//        }
//    }
//
//    @Test
//    public void testVertexLabelPropertyDoesNotExist() throws Exception {
//        this.sqlgGraph.addVertex(T.label, "A.A", "name", "aaa");
//        this.sqlgGraph.tx().commit();
//        Connection conn = this.sqlgGraph.tx().getConnection();
//        try (Statement statement = conn.createStatement()) {
//            statement.execute("ALTER TABLE " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") + "." +
//                    this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("V_A") + " DROP " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("name"));
//            this.sqlgGraph.tx().commit();
//        } catch (SQLException e) {
//            fail(e.getMessage());
//        }
//        this.sqlgGraph.close();
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            assertEquals(1, sqlgGraph1.getTopology().getValidationErrors().size());
//        }
//    }
//
//    @Test
//    public void testEdgeLabelPropertyDoesNotExist() throws Exception {
//        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A", "name", "aaa");
//        Vertex b = this.sqlgGraph.addVertex(T.label, "B.B", "name", "bbb");
//        a.addEdge("ab", b, "name", "asdadasdasd");
//        this.sqlgGraph.tx().commit();
//        Connection conn = this.sqlgGraph.tx().getConnection();
//        try (Statement statement = conn.createStatement()) {
//            statement.execute("ALTER TABLE " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") + "." +
//                    this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("E_ab") + " DROP " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("name"));
//            this.sqlgGraph.tx().commit();
//        } catch (SQLException e) {
//            fail(e.getMessage());
//        }
//        this.sqlgGraph.close();
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            assertEquals(1, sqlgGraph1.getTopology().getValidationErrors().size());
//        }
//    }

    @Test
    public void testIndexDoesNotExist() throws Exception {
        Assume.assumeTrue(isPostgres());
        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A", "name", "aaa");
        List<PropertyColumn> properties = new ArrayList<>(this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("A").get().getProperties().values());
        this.sqlgGraph.getTopology().getSchema("A").get().getVertexLabel("A").get().ensureIndexExists(IndexType.UNIQUE, properties);
        this.sqlgGraph.tx().commit();

        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            String indexName = this.sqlgGraph.getSqlDialect().indexName(SchemaTable.of("A", "A"), Topology.VERTEX_PREFIX, Collections.singletonList("name"));
            statement.execute("DROP INDEX " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("A") + "." +
                    this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(indexName));
            this.sqlgGraph.tx().commit();
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            assertEquals(1, sqlgGraph1.getTopology().getValidationErrors().size());
        }
    }

//    @Test
//    public void testGlobalUniqueIndexExist() throws Exception {
//        Map<String, PropertyType> properties = new HashMap<>();
//        properties.put("name1", PropertyType.STRING);
//        properties.put("name2", PropertyType.STRING);
//        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", properties);
//        properties.clear();
//        properties.put("name3", PropertyType.STRING);
//        properties.put("name4", PropertyType.STRING);
//        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B", properties);
//        properties.clear();
//        properties.put("name5", PropertyType.STRING);
//        properties.put("name6", PropertyType.STRING);
//        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, properties);
//        Set<PropertyColumn> globalUniqueIndexPropertyColumns = new HashSet<>();
//        globalUniqueIndexPropertyColumns.addAll(new HashSet<>(aVertexLabel.getProperties().values()));
//        globalUniqueIndexPropertyColumns.addAll(new HashSet<>(bVertexLabel.getProperties().values()));
//        globalUniqueIndexPropertyColumns.addAll(new HashSet<>(edgeLabel.getProperties().values()));
//        GlobalUniqueIndex globalUniqueIndex = this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(globalUniqueIndexPropertyColumns);
//        this.sqlgGraph.tx().commit();
//        Connection conn = this.sqlgGraph.tx().getConnection();
//        try (Statement statement = conn.createStatement()) {
//            statement.execute("DROP TABLE " + "\"" + Schema.GLOBAL_UNIQUE_INDEX_SCHEMA + "\".\"" + Topology.VERTEX_PREFIX + globalUniqueIndex.getName() + "\" CASCADE ");
//            this.sqlgGraph.tx().commit();
//        } catch (SQLException e) {
//            fail(e.getMessage());
//        }
//        this.sqlgGraph.close();
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            assertEquals(1, sqlgGraph1.getTopology().getValidationErrors().size());
//        }
//    }
}
