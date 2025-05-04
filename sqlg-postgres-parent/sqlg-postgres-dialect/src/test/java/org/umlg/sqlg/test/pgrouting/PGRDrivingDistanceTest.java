package org.umlg.sqlg.test.pgrouting;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.services.SqlgPGRoutingFactory;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SqlgEdge;
import org.umlg.sqlg.structure.SqlgVertex;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PGRDrivingDistanceTest extends BasePGRouting {

    private final static Logger LOGGER = LoggerFactory.getLogger(PGRDrivingDistanceTest.class);

    @Test
    public void g_V_call_distance() {
        loadPGRoutingSampleData();

        long start_vid = 1;
        long distance = 10;
        boolean directed = false;
        List<Path> result = sqlgGraph.traversal().E().hasLabel("edges").<Path>call(
                        SqlgPGRoutingFactory.NAME,
                        Map.of(
                                SqlgPGRoutingFactory.Params.FUNCTION, SqlgPGRoutingFactory.pgr_drivingDistance,
                                SqlgPGRoutingFactory.Params.START_VID, start_vid,
                                SqlgPGRoutingFactory.Params.DISTANCE, distance,
                                SqlgPGRoutingFactory.Params.DIRECTED, directed
                        ))
                .toList();
        String pgr_drivingDistance = """
                SELECT * FROM pgr_drivingDistance(
                  'SELECT id, source, target, cost, reverse_cost FROM edges',
                  %d, %d, %b);
                """.formatted(start_vid, distance, directed);

        assertPGDrivingDistance(result, pgr_drivingDistance);
    }

    private void assertPGDrivingDistance(List<Path> paths, String pgroutingSql) {
        Set<Long> pathEdges = new HashSet<>();
        Set<Long> leafVertices = new HashSet<>();
        for (Path path : paths) {
            int count = 1;
            for (Object o : path) {
                Assert.assertTrue(o instanceof SqlgVertex || o instanceof SqlgEdge);
                if (o instanceof SqlgEdge sqlgEdge) {
                    pathEdges.add(((RecordId)sqlgEdge.id()).sequenceId());
                }
                if (count == path.size()) {
                    Assert.assertTrue(o instanceof SqlgVertex);
                    leafVertices.add(((RecordId)((SqlgVertex)o).id()).sequenceId());
                }
                count++;
            }
        }
        Set<Long> rawLeafVertices = new HashSet<>();
        Set<Long> rawEdges = new HashSet<>();
        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(pgroutingSql);
            while (rs.next()) {
                long seq = rs.getLong("seq");
                long depth = rs.getLong("depth");
                long start_vid = rs.getLong("start_vid");
                long pred = rs.getLong("pred");
                rawLeafVertices.remove(pred);
                long node = rs.getLong("node");
                rawLeafVertices.add(node);
                long edge = rs.getLong("edge");
                if (edge != -1L) {
                    rawEdges.add(edge);
                }
                double cost = rs.getDouble("cost");
                double agg_cost = rs.getDouble("agg_cost");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals(pathEdges, rawEdges);
        Assert.assertEquals(leafVertices, rawLeafVertices);
    }
}
