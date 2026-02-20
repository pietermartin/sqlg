package org.umlg.sqlg.test.pgrouting;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.*;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.services.SqlgPGRoutingFactory;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.VertexLabel;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class PGRKspTest extends BasePGRouting {

    private static final Logger LOGGER = LoggerFactory.getLogger(PGRKspTest.class);

    @Test
    public void testKsp1() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
        }});
        aVertexLabel.ensureEdgeLabelExist("ab", aVertexLabel, new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("cost", PropertyDefinition.of(PropertyType.DOUBLE));
            put("reverse_cost", PropertyDefinition.of(PropertyType.DOUBLE));
        }});
        this.sqlgGraph.tx().commit();


        Vertex v6 = this.sqlgGraph.addVertex(T.label, "A", "name", "6");
        Vertex v7 = this.sqlgGraph.addVertex(T.label, "A", "name", "7");
        Vertex v11 = this.sqlgGraph.addVertex(T.label, "A", "name", "11");
        Vertex v16 = this.sqlgGraph.addVertex(T.label, "A", "name", "16");
        Vertex v15 = this.sqlgGraph.addVertex(T.label, "A", "name", "15");
        Vertex v10 = this.sqlgGraph.addVertex(T.label, "A", "name", "10");

        String edgeLabelName = "edges";
        Edge e4 = v6.addEdge(edgeLabelName, v7, "source", 6L, "target", 7L, "cost", 1D, "reverse_cost", 1D);
        Edge e8 = v7.addEdge(edgeLabelName, v11, "source", 7L, "target", 11L, "cost", 1D, "reverse_cost", 1D);
        Edge e9 = v11.addEdge(edgeLabelName, v16, "source", 11L, "target", 16L, "cost", 1D, "reverse_cost", 1D);
        Edge e16 = v15.addEdge(edgeLabelName, v16, "source", 15L, "target", 16L, "cost", 1D, "reverse_cost", 1D);
        Edge e3 = v10.addEdge(edgeLabelName, v15, "source", 10L, "target", 15L, "cost", 1D, "reverse_cost", 1D);
        Edge e2 = v6.addEdge(edgeLabelName, v10, "source", 6L, "target", 10L, "cost", 1D, "reverse_cost", 1D);

        this.sqlgGraph.tx().commit();

        List<Path> ksp_paths = this.sqlgGraph.traversal().E().hasLabel("edges")
                .ksp(((RecordId) (v6.id())).sequenceId(), ((RecordId) (v10.id())).sequenceId(), 2, false)
                .toList();
        Assert.assertEquals(2, ksp_paths.size());
        for (Path kspPath : ksp_paths) {
            LOGGER.debug(kspPath.toString());
        }
        Assert.assertEquals(3, ksp_paths.get(0).size());
        Assert.assertEquals(11, ksp_paths.get(1).size());

    }

    @Test
    public void testKsp2() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
        }});
        aVertexLabel.ensureEdgeLabelExist("ab", aVertexLabel, new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("cost", PropertyDefinition.of(PropertyType.DOUBLE));
            put("reverse_cost", PropertyDefinition.of(PropertyType.DOUBLE));
        }});
        this.sqlgGraph.tx().commit();


        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", "1");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A", "name", "2");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "A", "name", "3");
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "A", "name", "4");
        Vertex v5 = this.sqlgGraph.addVertex(T.label, "A", "name", "5");
        Vertex v6 = this.sqlgGraph.addVertex(T.label, "A", "name", "6");
        Vertex v7 = this.sqlgGraph.addVertex(T.label, "A", "name", "7");
        Vertex v8 = this.sqlgGraph.addVertex(T.label, "A", "name", "8");

        String edgeLabelName = "edges";
        Edge e1 = v1.addEdge(edgeLabelName, v2, "cost", 1D, "reverse_cost", 1D);
        Edge e2 = v2.addEdge(edgeLabelName, v3, "cost", 1D, "reverse_cost", 1D);
        Edge e3 = v3.addEdge(edgeLabelName, v4, "cost", 1D, "reverse_cost", 1D);
        Edge e4 = v1.addEdge(edgeLabelName, v5, "cost", 1D, "reverse_cost", 1D);
        Edge e5 = v5.addEdge(edgeLabelName, v6, "cost", 1D, "reverse_cost", 1D);
        Edge e6 = v6.addEdge(edgeLabelName, v4, "cost", 1D, "reverse_cost", 1D);
        Edge e7 = v1.addEdge(edgeLabelName, v7, "cost", 1D, "reverse_cost", 1D);
        Edge e8 = v7.addEdge(edgeLabelName, v8, "cost", 1D, "reverse_cost", 1D);
        Edge e9 = v8.addEdge(edgeLabelName, v4, "cost", 1D, "reverse_cost", 1D);

        this.sqlgGraph.tx().commit();

        List<Path> ksp_paths = this.sqlgGraph.traversal().E().hasLabel("edges")
                .ksp(((RecordId) (v1.id())).sequenceId(), ((RecordId) (v4.id())).sequenceId(), 2, false)
                .toList();
        Assert.assertEquals(2, ksp_paths.size());

        ksp_paths = this.sqlgGraph.traversal().E().hasLabel("edges")
                .ksp(((RecordId) (v1.id())).sequenceId(), ((RecordId) (v4.id())).sequenceId(), 3, false)
                .toList();
        Assert.assertEquals(3, ksp_paths.size());
    }

    @Test
    public void testKspOnSampleDataOneToOne() {
        loadPGRoutingSampleData();
        assertPGRoutingOneToOneSampleData(6, 17, 2, false);
    }

    @Test
    public void testKspOnSampleDataOneToMany() {
        loadPGRoutingSampleData();
        assertPGRoutingOneToManySampleData(6L, List.of(10L, 17L), 2, false);
    }

    @Test
    public void testKspOnSampleDataManyToOne() {
        loadPGRoutingSampleData();
        assertPGRoutingManyToOneSampleData(List.of(6L, 1L), 17L, 2, false);
    }

    @Test
    public void testKspOnSampleDataManyToMany() {
        loadPGRoutingSampleData();
        assertPGRoutingManyToManySampleData(List.of(6L, 1L), List.of(10L, 17L), 2, false);
    }

    private void assertPGRoutingManyToManySampleData(List<Long> start_vids, List<Long> end_vids, int k, boolean directed) {
        List<Path> paths = this.sqlgGraph.traversal().E().hasLabel("edges").ksp(
                        start_vids,
                        end_vids,
                        k,
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
                SELECT * FROM pgr_ksp(
                        'select "ID" as id, source, target, cost, reverse_cost from "E_edges" order by id',
                        %s, %s, %d, %b);
                """.formatted(startVids, endVids, k, directed);
        assertPGRoutingOnSampleData(paths, pgroutingSql);
    }

    private void assertPGRoutingOneToOneSampleData(long start_vid, long end_vid, int k, boolean directed) {
        List<Path> paths = this.sqlgGraph.traversal().E().hasLabel("edges").ksp(
                        start_vid,
                        end_vid,
                        k,
                        directed
                )
                .toList();
        String pgroutingSql = """
                SELECT * FROM pgr_ksp(
                        'select "ID" as id, source, target, cost, reverse_cost from "E_edges" order by "ID"',
                        %d, %d, %d, %b);
                """.formatted(start_vid, end_vid, k, directed);
        assertPGRoutingOnSampleData(paths, pgroutingSql);
    }

    private void assertPGRoutingOnSampleData(List<Path> paths, String pgroutingSql) {
        Connection connection = this.sqlgGraph.tx().getConnection();
        Map<Long, List<PGRKspTest.SampleData>> _sampleDataMap = new HashMap<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(pgroutingSql);
            while (rs.next()) {
                long start_vid = rs.getLong("start_vid");
                long end_vid = rs.getLong("end_vid");
                long path_id = rs.getLong("path_id");


                List<PGRKspTest.SampleData> sampleDataList = _sampleDataMap.computeIfAbsent(path_id, k -> new ArrayList<>());

                Long node = rs.getLong("node");
                Long edge = rs.getLong("edge");
                Double cost = rs.getDouble("cost");
                Double agg_cost = rs.getDouble("agg_cost");
                sampleDataList.add(new PGRKspTest.SampleData(node, edge, cost, agg_cost, start_vid, end_vid));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Map<String, List<PGRKspTest.SampleData>> sampleDataMap = new HashMap<>();
        for (Long l : _sampleDataMap.keySet()) {
            List<PGRKspTest.SampleData> sampleDataList = _sampleDataMap.get(l);
            StringBuilder key = new StringBuilder();
            int count = 1;
            for (SampleData sampleData : sampleDataList) {
                key.append(sampleData.node());
                if (count++ < sampleDataList.size()) {
                    key.append("_");
                }
            }
            sampleDataMap.put(key.toString(), sampleDataList);
        }


        if (sampleDataMap.isEmpty()) {
            Assert.assertTrue(paths.isEmpty());
        }
        Map<String, Path> startEndPath = new HashMap<>();
        for (Path path : paths) {
            Object start = path.get(0);
            Assert.assertTrue(start instanceof Vertex);
            Vertex startVertex = (Vertex) start;
            long start_vid = ((RecordId) startVertex.id()).sequenceId();
            Object end = path.get(path.size() - 1);
            Assert.assertTrue(end instanceof Vertex);
            Vertex endVertex = (Vertex) end;
            long end_vid = ((RecordId) endVertex.id()).sequenceId();
            StringBuilder key = new StringBuilder();
            int count = 1;
            for (Object o : path) {
                if (o instanceof SqlgVertex sqlgVertex) {
                    key.append(((RecordId)sqlgVertex.id()).sequenceId());
                    if (count < path.size() - 1) {
                        key.append("_");
                    }
                }
                count++;
            }
            startEndPath.put(key.toString(), path);
        }

        Assert.assertEquals(sampleDataMap.size(), startEndPath.size());

        for (String path_id : sampleDataMap.keySet()) {

            List<PGRKspTest.SampleData> sampleDataList = sampleDataMap.get(path_id);
            Path path = startEndPath.get(path_id);
            LOGGER.info(path.toString());
            Assert.assertEquals(path.size(), (sampleDataList.size() * 2L) - 1);

            int count = 0;
            int edgeCount = 0;
            Vertex previousVertex = null;
            for (Object o : path) {
                Element e = (Element) o;
                //start with vertex, edge, vertex, edge....
                PGRKspTest.SampleData sampleData = sampleDataList.get(edgeCount);
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

    private void assertPGRoutingOneToManySampleData(long start_vid, List<Long> end_vids, int k, boolean directed) {
        List<Path> paths = this.sqlgGraph.traversal().E().hasLabel("edges").<Path>ksp(
                        start_vid,
                        end_vids,
                        k,
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
                SELECT * FROM pgr_ksp(
                        'select "ID" as id, source, target, cost, reverse_cost from "E_edges" order by id',
                        %d, %s, %d, %b);
                """.formatted(start_vid, endVids, k, directed);
        assertPGRoutingOnSampleData(paths, pgroutingSql);
    }

    private void assertPGRoutingManyToOneSampleData(List<Long> start_vids, Long end_vid, int k, boolean directed) {
        List<Path> paths = this.sqlgGraph.traversal().E().hasLabel("edges").<Path>ksp(
                        start_vids,
                        end_vid,
                        k,
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
                SELECT * FROM pgr_ksp(
                        'select "ID" as id, source, target, cost, reverse_cost from "E_edges" order by id',
                        %s, %d, %d, %b);
                """.formatted(startVids, end_vid, k, directed);
        assertPGRoutingOnSampleData(paths, pgroutingSql);

    }

    private record SampleData(Long node, Long edge, Double cost, Double agg_cost, Long start_vid, Long end_vid) {
    }
}
