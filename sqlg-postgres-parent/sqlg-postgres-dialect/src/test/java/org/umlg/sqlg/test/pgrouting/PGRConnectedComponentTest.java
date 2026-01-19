package org.umlg.sqlg.test.pgrouting;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.services.SqlgPGRoutingFactory;
import org.umlg.sqlg.structure.RecordId;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PGRConnectedComponentTest extends BasePGRouting {
    private final static Logger LOGGER = LoggerFactory.getLogger(PGRConnectedComponentTest.class);

    @Test
    public void g_V_call_connectedComponent() {
        loadPGRoutingSampleData();

        List<Vertex> result = sqlgGraph.traversal().E().hasLabel("edges").<Vertex>call(
                        SqlgPGRoutingFactory.NAME,
                        Map.of(
                                SqlgPGRoutingFactory.Params.FUNCTION, SqlgPGRoutingFactory.pgr_connectedComponents
                        ))
                .toList();
        for (Vertex v: result) {
            long component = v.value(Graph.Hidden.hide(SqlgPGRoutingFactory.TRAVERSAL_COMPONENT));
            LOGGER.debug("{}: {}", component, v);
        }

        List<Vertex> components = this.sqlgGraph.traversal().E().hasLabel("edges").pgrConnectedComponent().toList();
        Assert.assertEquals(result, components);

        String pgr_drivingDistance = """
                SELECT * FROM pgr_connectedComponents(
                  'SELECT id, source, target, cost, reverse_cost FROM edges'
                  );
                """;
        assertPGConnectedComponents(result, pgr_drivingDistance);
    }

    private void assertPGConnectedComponents(List<Vertex> vertices, String pgroutingSql) {
        Set<Long> pathEdges = new HashSet<>();
        Set<Long> leafVertices = new HashSet<>();
        Set<Pair<Long, Long>> componentNodes = new HashSet<>();
        for (Vertex v: vertices) {
            long component = v.value(Graph.Hidden.hide(SqlgPGRoutingFactory.TRAVERSAL_COMPONENT));
            componentNodes.add(Pair.of(component, (((RecordId)v.id()).sequenceId())));
        }
        Set<Long> rawLeafVertices = new HashSet<>();
        Set<Long> rawEdges = new HashSet<>();
        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(pgroutingSql);
            while (rs.next()) {
                long component = rs.getLong("component");
                long node = rs.getLong("node");
                componentNodes.remove(Pair.of(component, node));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(componentNodes.isEmpty());
    }
}
