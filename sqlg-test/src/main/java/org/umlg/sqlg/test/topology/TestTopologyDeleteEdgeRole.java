package org.umlg.sqlg.test.topology;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.topology.EdgeRole;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.test.BaseTest;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class TestTopologyDeleteEdgeRole extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTopologyDeleteEdgeRole.class);

    @Parameterized.Parameter
    public String schema1;
    @Parameterized.Parameter(1)
    public String schema2;
    @Parameterized.Parameter(2)
    public String schema3;
    @Parameterized.Parameter(3)
    public boolean rollback;

    @Parameterized.Parameters(name = "{index}: schema1:{0}, schema2:{1}, schema3:{2}, rollback:{3}")
    public static Collection<Object[]> data() {
        List<Object[]> l = new ArrayList<>();
        String[] schema1s = new String[]{"public", "A"};
        String[] schema2s = new String[]{"public", "B"};
        String[] schema3s = new String[]{"public", "C"};
        boolean[] rollback = new boolean[]{true, false};
//        String[] schema1s = new String[]{"public"};
//        String[] schema2s = new String[]{"public"};
//        String[] schema3s = new String[]{"C"};
//        boolean[] rollback = new boolean[]{false};
        for (String s1 : schema1s) {
            for (String s2 : schema2s) {
                for (String s3 : schema3s) {
                    for (boolean r : rollback) {
                        l.add(new Object[]{s1, s2, s3, r});
                    }
                }
            }
        }
        return l;
    }

    @Before
    public void before() throws Exception {
        Assume.assumeFalse(isMariaDb());
        super.before();
    }

    @Test
    public void testDeleteVertexLabel() {
        Vertex a = this.sqlgGraph.addVertex(T.label, this.schema1 + ".A");
        Vertex b = this.sqlgGraph.addVertex(T.label, this.schema2 + ".B");
        Vertex c = this.sqlgGraph.addVertex(T.label, this.schema3 + ".C");
        a.addEdge("ab", b);
        a.addEdge("ac", c);
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ac").toList();
        Assert.assertEquals(1, vertices.size());
        this.sqlgGraph.getTopology().getSchema(this.schema3).orElseThrow().getVertexLabel("C").orElseThrow().remove();
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ac").toList();
        Assert.assertEquals(0, vertices.size());
        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema() && this.rollback) {
            this.sqlgGraph.tx().rollback();
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
            Assert.assertEquals(0, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ac").toList();
            Assert.assertEquals(0, vertices.size());
        } else {
            this.sqlgGraph.tx().commit();
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
            Assert.assertEquals(1, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ac").toList();
            Assert.assertEquals(0, vertices.size());
        }
    }

    @Test
    public void testDeleteEdgeRole() {
        Vertex a = this.sqlgGraph.addVertex(T.label, this.schema1 + ".A");
        Vertex b = this.sqlgGraph.addVertex(T.label, this.schema2 + ".B");
        Vertex c = this.sqlgGraph.addVertex(T.label, this.schema3 + ".C");
        a.addEdge("ab", b);
        a.addEdge("ab", c);
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
        Assert.assertEquals(2, vertices.size());
        this.sqlgGraph.getTopology().getSchema(this.schema3).orElseThrow().getVertexLabel("C").orElseThrow().remove();
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
        Assert.assertEquals(1, vertices.size());
        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema() && this.rollback) {
            this.sqlgGraph.tx().rollback();
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
            Assert.assertEquals(0, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema2 + ".B").in("ab").toList();
            Assert.assertEquals(0, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").toList();
            Assert.assertEquals(0, vertices.size());
        } else {
            this.sqlgGraph.tx().commit();
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
            Assert.assertEquals(1, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema2 + ".B").in("ab").toList();
            Assert.assertEquals(1, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").toList();
            Assert.assertEquals(0, vertices.size());
        }
    }

    @Test
    public void testDeleteEdgeRoleAfterCommitViaVertexLabelDelete() {
        Vertex a = this.sqlgGraph.addVertex(T.label, this.schema1 + ".A");
        Vertex b = this.sqlgGraph.addVertex(T.label, this.schema2 + ".B");
        Vertex c = this.sqlgGraph.addVertex(T.label, this.schema3 + ".C");
        a.addEdge("ab", b);
        a.addEdge("ab", c);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
        Assert.assertEquals(2, vertices.size());
        this.sqlgGraph.getTopology().getSchema(this.schema3).orElseThrow().getVertexLabel("C").orElseThrow().remove();
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema2 + ".B").in("ab").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").in("ab").toList();
        Assert.assertEquals(0, vertices.size());
        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema() && this.rollback) {
            this.sqlgGraph.tx().rollback();
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
            Assert.assertEquals(2, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema2 + ".B").in("ab").toList();
            Assert.assertEquals(1, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").in("ab").toList();
            Assert.assertEquals(1, vertices.size());
        } else {
            this.sqlgGraph.tx().commit();
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
            Assert.assertEquals(1, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema2 + ".B").in("ab").toList();
            Assert.assertEquals(1, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").in("ab").toList();
            Assert.assertEquals(0, vertices.size());

            //check rows in edge table, he edge from a to c must be deleted
            Connection connection = this.sqlgGraph.tx().getConnection();
            try (Statement statement = connection.createStatement()) {
                String sql = String.format("SELECT COUNT(*) FROM %s", this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema1) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("E_ab"));
                ResultSet rs = statement.executeQuery(sql);
                int count = 0;
                if (rs.next()) {
                    count = rs.getInt(1);
                }
                Assert.assertEquals(1, count);
            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
                Assert.fail(e.getMessage());
            }
        }
    }

    @Test
    public void testDeleteInEdgeRoleAfterCommitViaEdgeRoleDelete() {
        Vertex a = this.sqlgGraph.addVertex(T.label, this.schema1 + ".A");
        Vertex b = this.sqlgGraph.addVertex(T.label, this.schema2 + ".B");
        Vertex c = this.sqlgGraph.addVertex(T.label, this.schema3 + ".C");
        a.addEdge("ab", b);
        a.addEdge("ab", c);
        this.sqlgGraph.tx().commit();

        List<Vertex> sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "A")
                .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                .toList();
        Assert.assertEquals(1, sqlgSchemaVertices.size());
        sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, P.within("B","C"))
                .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                .toList();
        Assert.assertEquals(0, sqlgSchemaVertices.size());
        sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, P.within("B","C"))
                .out(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
                .toList();
        Assert.assertEquals(2, sqlgSchemaVertices.size());

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
        Assert.assertEquals(2, vertices.size());
        Map<String, EdgeRole> edgeRoleMap =  this.sqlgGraph.getTopology().getSchema(this.schema3).orElseThrow().getVertexLabel("C").orElseThrow().getInEdgeRoles();
        Assert.assertTrue(edgeRoleMap.containsKey(this.schema1 + ".ab"));
        EdgeRole edgeRole = edgeRoleMap.get(this.schema1 + ".ab");
        edgeRole.remove();

        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema2 + ".B").in("ab").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").in("ab").toList();
        Assert.assertEquals(0, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").toList();
        Assert.assertEquals(1, vertices.size());
        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema() && this.rollback) {
            this.sqlgGraph.tx().rollback();
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
            Assert.assertEquals(2, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema2 + ".B").in("ab").toList();
            Assert.assertEquals(1, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").in("ab").toList();
            Assert.assertEquals(1, vertices.size());

            sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "A")
                    .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .toList();
            Assert.assertEquals(1, sqlgSchemaVertices.size());
            sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, P.within("B","C"))
                    .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .toList();
            Assert.assertEquals(0, sqlgSchemaVertices.size());
            sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, P.within("B","C"))
                    .out(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
                    .toList();
            Assert.assertEquals(2, sqlgSchemaVertices.size());
        } else {
            this.sqlgGraph.tx().commit();
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
            Assert.assertEquals(1, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema2 + ".B").in("ab").toList();
            Assert.assertEquals(1, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").in("ab").toList();
            Assert.assertEquals(0, vertices.size());

            sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "A")
                    .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .toList();
            Assert.assertEquals(1, sqlgSchemaVertices.size());
            sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, P.within("B","C"))
                    .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .toList();
            Assert.assertEquals(0, sqlgSchemaVertices.size());
            sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, P.within("B","C"))
                    .out(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
                    .toList();
            Assert.assertEquals(1, sqlgSchemaVertices.size());


            //check rows in edge table, he edge from a to c must be deleted
            Connection connection = this.sqlgGraph.tx().getConnection();
            try (Statement statement = connection.createStatement()) {
                String sql = String.format("SELECT COUNT(*) FROM %s", this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema1) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("E_ab"));
                ResultSet rs = statement.executeQuery(sql);
                int count = 0;
                if (rs.next()) {
                    count = rs.getInt(1);
                }
                Assert.assertEquals(1, count);
            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
                Assert.fail(e.getMessage());
            }
        }
    }

    @Test
    public void testDeleteOutEdgeRoleAfterCommitViaEdgeRoleDelete() throws SQLException {
        Vertex a = this.sqlgGraph.addVertex(T.label, this.schema1 + ".A");
        Vertex b = this.sqlgGraph.addVertex(T.label, this.schema2 + ".B");
        Vertex c = this.sqlgGraph.addVertex(T.label, this.schema3 + ".C");
        a.addEdge("ab", b);
        c.addEdge("ab", a);
        this.sqlgGraph.tx().commit();

        //Assert C exists
        Connection connection = this.sqlgGraph.tx().getConnection();
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet resultSet = databaseMetaData.getTables(null, null, Topology.VERTEX_PREFIX + "C", new String[] {"TABLE"});
        Assert.assertTrue(resultSet.next());

        List<Vertex> sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "A")
                .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                .toList();
        Assert.assertEquals(1, sqlgSchemaVertices.size());
        sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "A")
                .out(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
                .toList();
        Assert.assertEquals(1, sqlgSchemaVertices.size());
        sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, P.within("C"))
                .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                .toList();
        Assert.assertEquals(1, sqlgSchemaVertices.size());
        sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, P.within("B"))
                .out(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
                .toList();
        Assert.assertEquals(1, sqlgSchemaVertices.size());

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").in("ab").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").out("ab").toList();
        Assert.assertEquals(1, vertices.size());
        Map<String, EdgeRole> edgeRoleMap =  this.sqlgGraph.getTopology().getSchema(this.schema3).orElseThrow().getVertexLabel("C").orElseThrow().getOutEdgeRoles();
        Assert.assertTrue(edgeRoleMap.containsKey(this.schema3 + ".ab"));
        EdgeRole edgeRole = edgeRoleMap.get(this.schema3 + ".ab");
        edgeRole.remove();

        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema2 + ".B").in("ab").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").in("ab").toList();
        Assert.assertEquals(0, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").toList();
        Assert.assertEquals(1, vertices.size());
        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema() && this.rollback) {
            this.sqlgGraph.tx().rollback();
            //Assert C exists
            connection = this.sqlgGraph.tx().getConnection();
            databaseMetaData = connection.getMetaData();
            resultSet = databaseMetaData.getTables(null, null, Topology.VERTEX_PREFIX + "C", new String[] {"TABLE"});
            Assert.assertTrue(resultSet.next());

            sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "A")
                    .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .toList();
            Assert.assertEquals(1, sqlgSchemaVertices.size());
            sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "A")
                    .out(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
                    .toList();
            Assert.assertEquals(1, sqlgSchemaVertices.size());
            sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, P.within("C"))
                    .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .toList();
            Assert.assertEquals(1, sqlgSchemaVertices.size());
            sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, P.within("B"))
                    .out(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
                    .toList();
            Assert.assertEquals(1, sqlgSchemaVertices.size());

            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
            Assert.assertEquals(1, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").in("ab").toList();
            Assert.assertEquals(1, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").out("ab").toList();
            Assert.assertEquals(1, vertices.size());

            //check rows in edge table, he edge from a to c is deleted and rolledBack
            connection = this.sqlgGraph.tx().getConnection();
            try (Statement statement = connection.createStatement()) {
                String sql = String.format("SELECT COUNT(*) FROM %s", this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema3) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("E_ab"));
                ResultSet rs = statement.executeQuery(sql);
                int count = 0;
                if (rs.next()) {
                    count = rs.getInt(1);
                }
                Assert.assertEquals(this.schema3.equals(this.sqlgGraph.getSqlDialect().getPublicSchema()) && this.schema1.equals(this.sqlgGraph.getSqlDialect().getPublicSchema()) ? 2 : 1, count);
            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
                Assert.fail(e.getMessage());
            }
        } else {
            this.sqlgGraph.tx().commit();
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
            Assert.assertEquals(1, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema2 + ".B").in("ab").toList();
            Assert.assertEquals(1, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").out("ab").toList();
            Assert.assertEquals(0, vertices.size());

            sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "A")
                    .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .toList();
            Assert.assertEquals(1, sqlgSchemaVertices.size());
            sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, P.within("B","C"))
                    .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .toList();
            Assert.assertEquals(0, sqlgSchemaVertices.size());
            sqlgSchemaVertices = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, P.within("B","C"))
                    .out(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
                    .toList();
            Assert.assertEquals(1, sqlgSchemaVertices.size());

            //Assert C exists
            connection = this.sqlgGraph.tx().getConnection();
            databaseMetaData = connection.getMetaData();
            resultSet = databaseMetaData.getTables(null, null, Topology.VERTEX_PREFIX + "C", new String[] {"TABLE"});
            Assert.assertTrue(resultSet.next());
        }
    }

    @Test
    public void testDeleteEdgeRoleViaSchemaDelete() {
        Assume.assumeTrue(this.schema3.equals("C"));
        Vertex a = this.sqlgGraph.addVertex(T.label, this.schema1 + ".A");
        Vertex b = this.sqlgGraph.addVertex(T.label, this.schema2 + ".B");
        Vertex c = this.sqlgGraph.addVertex(T.label, this.schema3 + ".C");
        a.addEdge("ab", b);
        a.addEdge("ab", c);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
        Assert.assertEquals(2, vertices.size());
        this.sqlgGraph.getTopology().getSchema(this.schema3).orElseThrow().remove();
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema2 + ".B").in("ab").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").in("ab").toList();
        Assert.assertEquals(0, vertices.size());
        if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema() && this.rollback) {
            this.sqlgGraph.tx().rollback();
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
            Assert.assertEquals(2, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema2 + ".B").in("ab").toList();
            Assert.assertEquals(1, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").in("ab").toList();
            Assert.assertEquals(1, vertices.size());
        } else {
            this.sqlgGraph.tx().commit();
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
            Assert.assertEquals(1, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema2 + ".B").in("ab").toList();
            Assert.assertEquals(1, vertices.size());
            vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema3 + ".C").in("ab").toList();
            Assert.assertEquals(0, vertices.size());
        }
    }

}
