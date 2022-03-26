package org.umlg.sqlg.test;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class TestForeignSchemaDocs {

    public static void main(String[] args) {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("jdbc.url", "jdbc:postgresql://localhost:5432/sqlgraphdb");
        properties.setProperty("jdbc.username", "postgres");
        properties.setProperty("jdbc.password", "postgres");
        SqlgGraph sqlgGraph = SqlgGraph.open(properties);
        SqlgUtil.dropDb(sqlgGraph);
        sqlgGraph.tx().commit();
        sqlgGraph = SqlgGraph.open(properties);

        Connection connection = sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw;");
            statement.execute("DROP SERVER IF EXISTS sqlgraph_fwd_server CASCADE;");
            String sql = String.format(
                    "CREATE SERVER \"%s\" FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '%s', dbname '%s', port '%d');",
                    "sqlgraph_fwd_server",
                    "localhost",
                    "sqlgraphdb_fdw",
                    5432
            );
            statement.execute(sql);
            sql = String.format(
                    "CREATE USER MAPPING FOR %s SERVER \"%s\" OPTIONS (user '%s', password '%s');",
                    "postgres",
                    "sqlgraph_fwd_server",
                    "postgres",
                    "postgres"
            );
            statement.execute(sql);
            sqlgGraph.tx().commit();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            sqlgGraph.tx().rollback();
        }

        PropertiesConfiguration propertiesForeign = new PropertiesConfiguration();
        propertiesForeign.setProperty("jdbc.url", "jdbc:postgresql://localhost:5432/sqlgraphdb_fdw");
        propertiesForeign.setProperty("jdbc.username", "postgres");
        propertiesForeign.setProperty("jdbc.password", "postgres");
        SqlgGraph sqlgGraphForeign = SqlgGraph.open(propertiesForeign);
        SqlgUtil.dropDb(sqlgGraphForeign);
        sqlgGraphForeign.tx().commit();
        sqlgGraphForeign = SqlgGraph.open(propertiesForeign);

        Schema foreignSchemaA = sqlgGraphForeign.getTopology().ensureSchemaExist("A");
        VertexLabel aVertexLabel = foreignSchemaA.ensureVertexLabelExist("A", new HashMap<>() {{
                    put("ID", PropertyType.UUID);
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of("ID"))
        );
        VertexLabel bVertexLabel = foreignSchemaA.ensureVertexLabelExist("B", new HashMap<>() {{
                    put("ID", PropertyType.UUID);
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of("ID"))
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new HashMap<>() {{
                    put("ID", PropertyType.UUID);
                    put("name", PropertyType.STRING);
                }}, ListOrderedSet.listOrderedSet(Set.of("ID"))
        );
        sqlgGraphForeign.tx().commit();

        connection = sqlgGraph.tx().getConnection();
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
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        sqlgGraph.tx().commit();
        sqlgGraph.getTopology().importForeignSchemas(Set.of(foreignSchemaA));

        Assert.assertTrue(sqlgGraph.getTopology().getSchema("A").isPresent());
        Assert.assertTrue(sqlgGraph.getTopology().getSchema("A").orElseThrow().getVertexLabel("A").isPresent());
        Assert.assertTrue(sqlgGraph.getTopology().getSchema("A").orElseThrow().getVertexLabel("B").isPresent());
        Assert.assertTrue(sqlgGraph.getTopology().getSchema("A").orElseThrow().getEdgeLabel("ab").isPresent());
        Vertex aVertex = sqlgGraph.addVertex(T.label, "A.A", "ID", UUID.randomUUID(), "name", "John");
        Vertex bVertex = sqlgGraph.addVertex(T.label, "A.B", "ID", UUID.randomUUID(), "name", "Joe");
        aVertex.addEdge("ab", bVertex, "ID", UUID.randomUUID(), "name", "myEdge");
        sqlgGraph.tx().commit();
        Assert.assertEquals(1L, sqlgGraph.traversal().V()
                .hasLabel("A.A")
                .has("name", P.eq("John"))
                .out("ab")
                .count().next(), 0);
    }

    public static void main4(String[] args) {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("jdbc.url", "jdbc:postgresql://localhost:5432/sqlgraphdb");
        properties.setProperty("jdbc.username", "postgres");
        properties.setProperty("jdbc.password", "postgres");
        SqlgGraph sqlgGraph = SqlgGraph.open(properties);
        SqlgUtil.dropDb(sqlgGraph);
        sqlgGraph.tx().commit();
        sqlgGraph = SqlgGraph.open(properties);

        Connection connection = sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw;");
            statement.execute("DROP SERVER IF EXISTS sqlgraph_fwd_server CASCADE;");
            String sql = String.format(
                    "CREATE SERVER \"%s\" FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '%s', dbname '%s', port '%d');",
                    "sqlgraph_fwd_server",
                    "localhost",
                    "sqlgraphdb_fdw",
                    5432
            );
            statement.execute(sql);
            sql = String.format(
                    "CREATE USER MAPPING FOR %s SERVER \"%s\" OPTIONS (user '%s', password '%s');",
                    "postgres",
                    "sqlgraph_fwd_server",
                    "postgres",
                    "postgres"
            );
            statement.execute(sql);
            sqlgGraph.tx().commit();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            sqlgGraph.tx().rollback();
        }

        PropertiesConfiguration propertiesForeign = new PropertiesConfiguration();
        propertiesForeign.setProperty("jdbc.url", "jdbc:postgresql://localhost:5432/sqlgraphdb_fdw");
        propertiesForeign.setProperty("jdbc.username", "postgres");
        propertiesForeign.setProperty("jdbc.password", "postgres");
        SqlgGraph sqlgGraphForeign = SqlgGraph.open(propertiesForeign);
        SqlgUtil.dropDb(sqlgGraphForeign);
        sqlgGraphForeign.tx().commit();
        sqlgGraphForeign = SqlgGraph.open(propertiesForeign);

        Schema foreignSchemaA = sqlgGraphForeign.getTopology().ensureSchemaExist("A");
        VertexLabel aVertexLabel = foreignSchemaA.ensureVertexLabelExist("A",
                new HashMap<>() {{
                    put("name", PropertyType.STRING);
                }}
        );
        VertexLabel bVertexLabel = foreignSchemaA.ensureVertexLabelExist("B",
                new HashMap<>() {{
                    put("name", PropertyType.STRING);
                }}
        );
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new HashMap<>() {{
                    put("name", PropertyType.STRING);
                }}
        );
        sqlgGraphForeign.tx().commit();

        connection = sqlgGraph.tx().getConnection();
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
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        sqlgGraph.tx().commit();
        sqlgGraph.getTopology().importForeignSchemas(Set.of(foreignSchemaA));

        Assert.assertTrue(sqlgGraph.getTopology().getSchema("A").isPresent());
        Assert.assertTrue(sqlgGraph.getTopology().getSchema("A").orElseThrow().getVertexLabel("A").isPresent());
        Assert.assertTrue(sqlgGraph.getTopology().getSchema("A").orElseThrow().getVertexLabel("B").isPresent());
        Assert.assertTrue(sqlgGraph.getTopology().getSchema("A").orElseThrow().getEdgeLabel("ab").isPresent());
        Vertex aVertex = sqlgGraphForeign.addVertex(T.label, "A.A", "name", "John");
        Vertex bVertex = sqlgGraphForeign.addVertex(T.label, "A.B", "name", "Joe");
        aVertex.addEdge("ab", bVertex, "name", "myEdge");
        sqlgGraphForeign.tx().commit();
        
        Assert.assertEquals(1L, sqlgGraph.traversal().V()
                .hasLabel("A.A")
                .has("name", P.eq("John"))
                .out("ab")
                .count().next(), 0);
    }
}
