package org.umlg.sqlg.test.pgrouting;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.services.SqlgPGRoutingFactory;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.topology.Schema;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PGRDijkstraSampleGraphTest extends BasePGRouting {

    private final static Logger LOGGER = LoggerFactory.getLogger(PGRDijkstraSampleGraphTest.class);

    @Test
    public void g_V_call_dijkstra_one2one_sampleData_5_to_10() {
        Schema customSchema = this.sqlgGraph.getTopology().ensureSchemaExist("custom");
        loadPGRoutingSampleData(customSchema);
        assertPGRoutingOneToOneSampleDataInCustomSchema(customSchema, 6L, 10L, true);
        assertPGRoutingOneToOneSampleDataInCustomSchema(customSchema, 6L, 10L, false);
    }

    @Test
    public void g_V_call_dijkstra_one2one_sampleDataInCustomSchema() {
        Schema customSchema = this.sqlgGraph.getTopology().ensureSchemaExist("custom");
        loadPGRoutingSampleData(customSchema);
        for (Long start_vid : List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L)) {
            for (Long end_vid : List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L)) {
                LOGGER.info("start_vid: {}, end_vid: {}", start_vid, end_vid);
                assertPGRoutingOneToOneSampleDataInCustomSchema(customSchema, start_vid, end_vid, true);
                assertPGRoutingOneToOneSampleDataInCustomSchema(customSchema, start_vid, end_vid, false);
            }
        }
        assertPGRoutingOneToOneSampleDataInCustomSchema(customSchema, 6L, 10L, false);
    }

    @Test
    public void g_V_call_dijkstra_one2one_sampleDataInCustomSchema2() {
        Schema customSchema1 = this.sqlgGraph.getTopology().ensureSchemaExist("custom1");
        Schema customSchema2 = this.sqlgGraph.getTopology().ensureSchemaExist("custom2");
        loadPGRoutingSampleData(customSchema1);
        loadPGRoutingSampleData(customSchema2);
        for (Long start_vid : List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L)) {
            for (Long end_vid : List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L)) {
                LOGGER.info("start_vid: {}, end_vid: {}", start_vid, end_vid);
                assertPGRoutingOneToOneSampleDataInCustomSchema(customSchema1, start_vid, end_vid, true);
                assertPGRoutingOneToOneSampleDataInCustomSchema(customSchema1, start_vid, end_vid, false);
            }
        }
        assertPGRoutingOneToOneSampleDataInCustomSchema(customSchema1, 6L, 10L, false);
    }

    @Test
    public void g_V_call_dijkstra_one2one_sampleData() {
        loadPGRoutingSampleData();
        for (Long start_vid : List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L)) {
            for (Long end_vid : List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L)) {
                LOGGER.info("start_vid: {}, end_vid: {}", start_vid, end_vid);
                assertPGRoutingOneToOneSampleData(start_vid, end_vid, true);
                assertPGRoutingOneToOneSampleData(start_vid, end_vid, false);
            }
        }
        assertPGRoutingOneToOneSampleData(6L, 10L, false);
    }

    @Test
    public void g_V_call_dijkstra_one2many_sampleData() {
        loadPGRoutingSampleData();
        assertPGRoutingOneToManySampleData(6L, List.of(10L, 17L), true);
        for (Long start_vid : List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L)) {
            assertPGRoutingOneToManySampleData(start_vid, List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L), true);
            assertPGRoutingOneToManySampleData(start_vid, List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L), false);
        }
    }

    @Test
    public void g_V_call_dijkstra_many2one_sampleData() {
        loadPGRoutingSampleData();
        for (Long end_vid : List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L)) {
            assertPGRoutingManyToOneSampleData(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L), end_vid, true);
            assertPGRoutingManyToOneSampleData(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L), end_vid, false);
        }
    }

    @Test
    public void g_V_call_dijkstra_many2many_sampleData() {
        loadPGRoutingSampleData();
        assertPGRoutingManyToManySampleData(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L), List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L), true);
        assertPGRoutingManyToManySampleData(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L), List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L), false);
    }

    private void assertPGRoutingManyToManySampleData(List<Long> start_vids, List<Long> end_vids, boolean directed) {
        List<Path> paths = this.sqlgGraph.traversal().E().hasLabel("edges").dijkstra(
                        start_vids,
                        end_vids,
                        directed
                )
                .toList();
        String startVids;
        if (start_vids.size() == 1) {
            startVids = start_vids.get(0).toString();
        } else {
            startVids = "ARRAY[" + String.join(",", start_vids.stream().map(Object::toString).toArray(String[]::new)) + "]";
        }
        String endVids;
        if (end_vids.size() == 1) {
            endVids = end_vids.get(0).toString();
        } else {
            endVids = "ARRAY[" + String.join(",", end_vids.stream().map(Object::toString).toArray(String[]::new)) + "]";
        }
        String pgroutingSql = """
                SELECT * FROM pgr_Dijkstra(
                        'select "ID" as id, source, target, cost, reverse_cost from "E_edges" order by id',
                        %s, %s, %b);
                """.formatted(startVids, endVids, directed);
        assertPGRoutingOnSampleData(paths, pgroutingSql);
    }

    private void assertPGRoutingManyToOneSampleData(List<Long> start_vids, Long end_vid, boolean directed) {
        List<Path> paths = this.sqlgGraph.traversal().E().hasLabel("edges").<Path>dijkstra(
                        start_vids,
                        end_vid,
                        directed
                )
                .toList();
        String startVids;
        if (start_vids.size() == 1) {
            startVids = start_vids.get(0).toString();
        } else {
            startVids = "ARRAY[" + String.join(",", start_vids.stream().map(Object::toString).toArray(String[]::new)) + "]";
        }
        String pgroutingSql = """
                SELECT * FROM pgr_Dijkstra(
                        'select "ID" as id, source, target, cost, reverse_cost from "E_edges" order by id',
                        %s, %d, %b);
                """.formatted(startVids, end_vid, directed);
        assertPGRoutingOnSampleData(paths, pgroutingSql);

    }

    private void assertPGRoutingOneToManySampleData(long start_vid, List<Long> end_vids, boolean directed) {
        List<Path> paths = this.sqlgGraph.traversal().E().hasLabel("edges").<Path>dijkstra(
                        start_vid,
                        end_vids,
                        directed
                )
                .toList();
        String endVids;
        if (end_vids.size() == 1) {
            endVids = end_vids.get(0).toString();
        } else {
            endVids = "ARRAY[" + String.join(",", end_vids.stream().map(Object::toString).toArray(String[]::new)) + "]";
        }
        String pgroutingSql = """
                SELECT * FROM pgr_Dijkstra(
                        'select "ID" as id, source, target, cost, reverse_cost from "E_edges" order by id',
                        %d, %s, %b);
                """.formatted(start_vid, endVids, directed);
        assertPGRoutingOnSampleData(paths, pgroutingSql);
    }

    private void assertPGRoutingOneToOneSampleDataInCustomSchema(Schema schema, long start_vid, long end_vid, boolean directed) {
        List<Path> paths = this.sqlgGraph.traversal().E().hasLabel(schema.getName() + ".edges").dijkstra(
                        start_vid,
                        end_vid,
                        directed
                )
                .toList();
        String pgroutingSql = """
                SELECT * FROM pgr_Dijkstra(
                        'select "ID" as id, source, target, cost, reverse_cost from "%s"."E_edges" order by "ID"',
                        %d, %d, %b);
                """.formatted(schema.getName(), start_vid, end_vid, directed);
        assertPGRoutingOnSampleData(paths, pgroutingSql);
    }

    private void assertPGRoutingOneToOneSampleData(long start_vid, long end_vid, boolean directed) {
        List<Path> paths = this.sqlgGraph.traversal().E().hasLabel("edges").dijkstra(
                        start_vid,
                        end_vid,
                        directed
                )
                .toList();
        String pgroutingSql = """
                SELECT * FROM pgr_Dijkstra(
                        'select "ID" as id, source, target, cost, reverse_cost from "E_edges" order by "ID"',
                        %d, %d, %b);
                """.formatted(start_vid, end_vid, directed);
        assertPGRoutingOnSampleData(paths, pgroutingSql);
    }

    private void assertPGRoutingOnSampleData(List<Path> paths, String pgroutingSql) {
        Connection connection = this.sqlgGraph.tx().getConnection();
        Map<SqlgPGRoutingFactory.StartEndVid, List<SampleData>> sampleDataMap = new HashMap<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(pgroutingSql);
            while (rs.next()) {
                long start_vid = rs.getLong("start_vid");
                long end_vid = rs.getLong("end_vid");
                SqlgPGRoutingFactory.StartEndVid startEndVid = new SqlgPGRoutingFactory.StartEndVid(start_vid, end_vid);
                List<SampleData> sampleDataList = sampleDataMap.computeIfAbsent(startEndVid, k -> new ArrayList<>());

                Long node = rs.getLong("node");
                Long edge = rs.getLong("edge");
                Double cost = rs.getDouble("cost");
                Double agg_cost = rs.getDouble("agg_cost");
                sampleDataList.add(new SampleData(node, edge, cost, agg_cost, start_vid, end_vid));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (sampleDataMap.isEmpty()) {
            Assert.assertTrue(paths.isEmpty());
        }
        Map<SqlgPGRoutingFactory.StartEndVid, Path> startEndPath = new HashMap<>();
        for (Path path : paths) {
            Object start = path.get(0);
            Assert.assertTrue(start instanceof Vertex);
            Vertex startVertex = (Vertex) start;
            long start_vid = ((RecordId) startVertex.id()).sequenceId();
            Object end = path.get(path.size() - 1);
            Assert.assertTrue(end instanceof Vertex);
            Vertex endVertex = (Vertex) end;
            long end_vid = ((RecordId) endVertex.id()).sequenceId();
            SqlgPGRoutingFactory.StartEndVid startEndVid = new SqlgPGRoutingFactory.StartEndVid(start_vid, end_vid);
            startEndPath.put(startEndVid, path);
        }

        Assert.assertEquals(sampleDataMap.size(), startEndPath.size());

        for (SqlgPGRoutingFactory.StartEndVid startEndVid : sampleDataMap.keySet()) {

            List<SampleData> sampleDataList = sampleDataMap.get(startEndVid);
            Path path = startEndPath.get(startEndVid);
            LOGGER.info(path.toString());
            Assert.assertEquals(path.size(), (sampleDataList.size() * 2L) - 1);

            int count = 0;
            int edgeCount = 0;
            Vertex previousVertex = null;
            for (Object o : path) {
                Element e = (Element) o;
                //start with vertex, edge, vertex, edge....
                SampleData sampleData = sampleDataList.get(edgeCount);
                if (count % 2 == 0) {
                    Assert.assertTrue(e instanceof Vertex);
                    previousVertex = (Vertex) e;
                } else {
                    Assert.assertTrue(e instanceof Edge);
                    Assert.assertNotNull(previousVertex);
                    edgeCount++;

                    long previousVertexId = ((RecordId) previousVertex.id()).sequenceId();
                    long edgeId = ((RecordId) e.id()).sequenceId();
                    double cost = e.value(Graph.Hidden.hide(SqlgPGRoutingFactory.TRAVERSAL_COST));
                    double agg_cost = e.value(Graph.Hidden.hide(SqlgPGRoutingFactory.TRAVERSAL_AGG_COST));

                    Assert.assertEquals(previousVertexId, sampleData.node().longValue());
                    Assert.assertEquals(edgeId, sampleData.edge().longValue());
                    Assert.assertEquals(cost, sampleData.cost(), 0D);
                    Assert.assertEquals(agg_cost, sampleData.agg_cost(), 0D);
                }
                count++;
            }
        }
    }

    private record SampleData(Long node, Long edge, Double cost, Double agg_cost, Long start_vid, Long end_vid) {
    }


}
