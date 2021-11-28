package org.umlg.sqlg.test.roles;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.TopologyChangeAction;
import org.umlg.sqlg.structure.TopologyInf;
import org.umlg.sqlg.structure.TopologyListener;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/07/21
 */
public class TestReadOnlyRole extends BaseTest {

    @Before
    public void before() throws Exception {
        //H2 is locking on opening readOnly connection.
        //TODO remove hsqldb depending on https://github.com/pietermartin/sqlg/issues/411
        Assume.assumeFalse(isH2() || isHsqldb());
        super.before();
    }

    @Test
    public void testReadOnlyRoleOnPublicTable() throws ConfigurationException {
        TopologyGrantListener topologyGrantListener = new TopologyGrantListener();
        this.sqlgGraph.getTopology().registerListener(topologyGrantListener);

        this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        this.sqlgGraph.tx().commit();

        Configurations configs = new Configurations();
        Configuration readOnlyConfiguration = configs.properties("sqlg.readonly.properties");
        try (SqlgGraph readOnlyGraph = SqlgGraph.open(readOnlyConfiguration)) {
            List<Vertex> vertices = readOnlyGraph.traversal().V().hasLabel("A").toList();
            Assert.assertEquals(2, vertices.size());
            try {
                readOnlyGraph.addVertex(T.label, "A", "name", "a3");
                Assert.fail("Graph is suppose to be readOnly");
            } catch (Exception ignore) {
            }
        }
    }

    @Test
    public void testReadOnlyRoleOnSchema() throws ConfigurationException {
        TopologyGrantListener topologyGrantListener = new TopologyGrantListener();
        this.sqlgGraph.getTopology().registerListener(topologyGrantListener);

        this.sqlgGraph.addVertex(T.label, "A.A", "name", "a1");
        this.sqlgGraph.addVertex(T.label, "A.A", "name", "a2");
        this.sqlgGraph.tx().commit();

        Configurations configs = new Configurations();
        Configuration readOnlyConfiguration = configs.properties("sqlg.readonly.properties");
        try (SqlgGraph readOnlyGraph = SqlgGraph.open(readOnlyConfiguration)) {
            List<Vertex> vertices = readOnlyGraph.traversal().V().hasLabel("A.A").toList();
            Assert.assertEquals(2, vertices.size());
            try {
                readOnlyGraph.addVertex(T.label, "A.A", "name", "a3");
                Assert.fail("Graph is suppose to be readOnly");
            } catch (Exception ignore) {
            }
        }
    }

    @Test
    public void testReadOnlyRoleOnSchemasVertexLabelAndEdgeLabel() throws ConfigurationException {
        TopologyGrantListener topologyGrantListener = new TopologyGrantListener();
        this.sqlgGraph.getTopology().registerListener(topologyGrantListener);

        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A", "name", "a1");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B.B", "name", "b1");
        a.addEdge("ab", b, "name", "ab1");
        a.addEdge("ab", b, "name", "ab1");
        this.sqlgGraph.tx().commit();

        Configurations configs = new Configurations();
        Configuration readOnlyConfiguration = configs.properties("sqlg.readonly.properties");
        try (SqlgGraph readOnlyGraph = SqlgGraph.open(readOnlyConfiguration)) {
            List<Edge> edges = readOnlyGraph.traversal().V().hasLabel("A.A").outE("ab").toList();
            Assert.assertEquals(2, edges.size());
            List<Vertex> vertices = readOnlyGraph.traversal().V().hasLabel("A.A").out("ab").toList();
            Assert.assertEquals(2, vertices.size());
            try {
                Vertex vertexA = readOnlyGraph.traversal().V().hasLabel("A.A").has("name", "a1").next();
                Vertex vertexB = readOnlyGraph.traversal().V().hasLabel("B.B").has("name", "b1").next();
                vertexA.addEdge("ab", vertexB, "name", "ab2");
                Assert.fail("Graph is suppose to be readOnly");
            } catch (Exception ignore) {
            }
        }
    }

    public class TopologyGrantListener implements TopologyListener {

        TopologyGrantListener() {
        }

        @Override
        public void change(TopologyInf topologyInf, TopologyInf oldValue, TopologyChangeAction action) {
            switch (action) {
                case CREATE:
                    if (topologyInf instanceof VertexLabel) {
                        VertexLabel vertexLabel = (VertexLabel) topologyInf;
                        Connection conn = sqlgGraph.tx().getConnection();
                        try (Statement statement = conn.createStatement()) {
                            String sql = "";
                            if (isPostgres()) {
                                 sql = "GRANT SELECT ON " +
                                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(vertexLabel.getSchema().getName()) + "." +
                                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.VERTEX_PREFIX + vertexLabel.getName()) + " TO \"sqlgReadOnly\"";
                                statement.execute(sql);
                            } else if (isHsqldb()) {
                                sql = "GRANT SELECT ON TABLE " +
                                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(vertexLabel.getSchema().getName()) + "." +
                                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.VERTEX_PREFIX + vertexLabel.getName()) + " TO READ_ONLY";
                                statement.execute(sql);
                            } else if (isMariaDb() || isMysql()) {
                                //nothing to do. MariaDb user is created with select all rights
                            } else if (isMsSqlServer()) {
                                sql = "GRANT SELECT ON OBJECT:: " +
                                        vertexLabel.getSchema().getName() + "." +
                                        Topology.VERTEX_PREFIX + vertexLabel.getName() + " TO sqlgReadOnly;";
                                statement.execute(sql);
                            } else if (isH2()) {
                                sql = "GRANT SELECT ON " +
                                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(vertexLabel.getSchema().getName()) + "." +
                                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.VERTEX_PREFIX + vertexLabel.getName()) + " TO READ_ONLY";
                                statement.execute(sql);
                            } else {
                                Assert.fail("Not handled");
                            }
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (topologyInf instanceof EdgeLabel) {
                        EdgeLabel edgeLabel = (EdgeLabel) topologyInf;
                        Connection conn = sqlgGraph.tx().getConnection();
                        try (Statement statement = conn.createStatement()) {
                            String sql = "";
                            if (isPostgres()) {
                                sql = "GRANT SELECT ON " +
                                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeLabel.getSchema().getName()) + "." +
                                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.EDGE_PREFIX + edgeLabel.getName()) + " TO \"sqlgReadOnly\"";
                                statement.execute(sql);
                            } else if (isHsqldb()) {
                                sql = "GRANT SELECT ON TABLE " +
                                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeLabel.getSchema().getName()) + "." +
                                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.EDGE_PREFIX + edgeLabel.getName()) + " TO READ_ONLY";
                                statement.execute(sql);
                            } else if (isMariaDb() || isMysql()) {
                                //nothing to do. MariaDb user is created with select all rights
                            } else if (isMsSqlServer()) {
                                sql = "GRANT SELECT ON OBJECT::" +
                                        edgeLabel.getSchema().getName() + "." +
                                        Topology.EDGE_PREFIX + edgeLabel.getName() + " TO sqlgReadOnly;";
                                statement.execute(sql);
                            } else if (isH2()) {
                                sql = "GRANT SELECT ON " +
                                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeLabel.getSchema().getName()) + "." +
                                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.EDGE_PREFIX + edgeLabel.getName()) + " TO READ_ONLY";
                                statement.execute(sql);
                            } else {
                                Assert.fail("Not handled");
                            }
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (topologyInf instanceof Schema) {
                        Schema schema = (Schema) topologyInf;
                        Connection conn = sqlgGraph.tx().getConnection();
                        try (Statement statement = conn.createStatement()) {
                            if (isPostgres()) {
                                String sql = "GRANT USAGE ON SCHEMA  " + sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema.getName()) + " TO \"sqlgReadOnly\"";
                                statement.execute(sql);
                            } else if (isMsSqlServer()) {
                                String sql = "GRANT SELECT ON SCHEMA :: " + sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema.getName()) + " TO sqlgReadOnly";
                                statement.execute(sql);
                            }
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    break;
                case ADD_IN_VERTEX_LABELTO_EDGE:
                    break;
                case DELETE:
                    break;
            }
        }
    }
}
