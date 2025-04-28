package org.umlg.sqlg.test.pgrouting;

import net.postgis.jdbc.geometry.LineString;
import net.postgis.jdbc.geometry.Point;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.services.SqlgPGRoutingFactory;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class SqlgGraphPGRoutingTest extends BaseTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(SqlgGraphPGRoutingTest.class);

    @Before
    public void registerServices() throws Exception {
        super.before();
        sqlgGraph.getServiceRegistry().registerService(new SqlgPGRoutingFactory(sqlgGraph));
    }

    //    @Test
    public void g_V_call_degree_centrality() {
        List<Object> result = sqlgGraph.traversal().V().hasLabel("wiki").as("v").call(SqlgPGRoutingFactory.NAME, Map.of()).toList();
        for (Object o : result) {
            System.out.println(o.toString());
        }
    }

    //    @Test
    public void g_V_call_dijkstra() {
        loadWikiGraph();

        List<Path> result = sqlgGraph.traversal().E().hasLabel("connects").<Path>call(
                        SqlgPGRoutingFactory.NAME,
                        Map.of(
                                SqlgPGRoutingFactory.Params.FUNCTION, SqlgPGRoutingFactory.pgr_dijkstra,
                                SqlgPGRoutingFactory.Params.START_VID, 1L,
                                SqlgPGRoutingFactory.Params.END_VID, 5L,
                                SqlgPGRoutingFactory.Params.DIRECTED, false
                        ))
                .toList();
        Assert.assertEquals(1, result.size());
        Path p = result.get(0);
        LOGGER.debug(p.toString());
        Assert.assertTrue(p.get(1) instanceof Edge && ((Edge) p.get(1)).value("source").equals(1) && ((Edge) p.get(1)).value("target").equals(3));
        Assert.assertTrue(p.get(3) instanceof Edge && ((Edge) p.get(3)).value("source").equals(3) && ((Edge) p.get(3)).value("target").equals(6));
        Assert.assertTrue(p.get(5) instanceof Edge && ((Edge) p.get(5)).value("source").equals(5) && ((Edge) p.get(5)).value("target").equals(6));
        Assert.assertTrue(
                p.size() == 7 &&
                        p.get(0) instanceof Vertex && ((Vertex) p.get(0)).label().equals("wiki") && ((Vertex) p.get(0)).value("name").equals("1") && ((Vertex) p.get(0)).value("_index").equals(1) &&
                        p.get(1) instanceof Edge && ((Edge) p.get(1)).label().equals("connects") && ((Edge) p.get(1)).value("source").equals(1) && ((Edge) p.get(1)).value("target").equals(3) &&
                        p.get(2) instanceof Vertex && ((Vertex) p.get(0)).label().equals("wiki") && ((Vertex) p.get(2)).value("name").equals("3") && ((Vertex) p.get(2)).value("_index").equals(3) &&
                        p.get(3) instanceof Edge && ((Edge) p.get(1)).label().equals("connects") && ((Edge) p.get(3)).value("source").equals(3) && ((Edge) p.get(3)).value("target").equals(6) &&
                        p.get(4) instanceof Vertex && ((Vertex) p.get(0)).label().equals("wiki") && ((Vertex) p.get(4)).value("name").equals("6") && ((Vertex) p.get(4)).value("_index").equals(6) &&
                        p.get(5) instanceof Edge && ((Edge) p.get(1)).label().equals("connects") && ((Edge) p.get(5)).value("source").equals(5) && ((Edge) p.get(5)).value("target").equals(6) &&
                        p.get(6) instanceof Vertex && ((Vertex) p.get(0)).label().equals("wiki") && ((Vertex) p.get(6)).value("name").equals("5") && ((Vertex) p.get(6)).value("_index").equals(5)
        );
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
        List<Path> paths = this.sqlgGraph.traversal().E().hasLabel("edges").<Path>call(
                        SqlgPGRoutingFactory.NAME,
                        Map.of(
                                SqlgPGRoutingFactory.Params.FUNCTION, SqlgPGRoutingFactory.pgr_dijkstra,
                                SqlgPGRoutingFactory.Params.START_VIDS, start_vids,
                                SqlgPGRoutingFactory.Params.END_VIDS, end_vids,
                                SqlgPGRoutingFactory.Params.DIRECTED, directed
                        ))
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
                        'select id, source, target, cost, reverse_cost from edges order by id',
                        %s, %s, %b);
                """.formatted(startVids, endVids, directed);
        assertPGRoutingOnSampleData(paths, pgroutingSql);
    }

    private void assertPGRoutingManyToOneSampleData(List<Long> start_vids, Long end_vid, boolean directed) {
        List<Path> paths = this.sqlgGraph.traversal().E().hasLabel("edges").<Path>call(
                        SqlgPGRoutingFactory.NAME,
                        Map.of(
                                SqlgPGRoutingFactory.Params.FUNCTION, SqlgPGRoutingFactory.pgr_dijkstra,
                                SqlgPGRoutingFactory.Params.START_VIDS, start_vids,
                                SqlgPGRoutingFactory.Params.END_VID, end_vid,
                                SqlgPGRoutingFactory.Params.DIRECTED, directed
                        ))
                .toList();
        String startVids;
        if (start_vids.size() == 1) {
            startVids = start_vids.get(0).toString();
        } else {
            startVids = "ARRAY[" + String.join(",", start_vids.stream().map(Object::toString).toArray(String[]::new)) + "]";
        }
        String pgroutingSql = """
                SELECT * FROM pgr_Dijkstra(
                        'select id, source, target, cost, reverse_cost from edges order by id',
                        %s, %d, %b);
                """.formatted(startVids, end_vid, directed);
        assertPGRoutingOnSampleData(paths, pgroutingSql);

    }

    private void assertPGRoutingOneToManySampleData(long start_vid, List<Long> end_vids, boolean directed) {
        List<Path> paths = this.sqlgGraph.traversal().E().hasLabel("edges").<Path>call(
                        SqlgPGRoutingFactory.NAME,
                        Map.of(
                                SqlgPGRoutingFactory.Params.FUNCTION, SqlgPGRoutingFactory.pgr_dijkstra,
                                SqlgPGRoutingFactory.Params.START_VID, start_vid,
                                SqlgPGRoutingFactory.Params.END_VIDS, end_vids,
                                SqlgPGRoutingFactory.Params.DIRECTED, directed
                        ))
                .toList();
        String endVids;
        if (end_vids.size() == 1) {
            endVids = end_vids.get(0).toString();
        } else {
            endVids = "ARRAY[" + String.join(",", end_vids.stream().map(Object::toString).toArray(String[]::new)) + "]";
        }
        String pgroutingSql = """
                SELECT * FROM pgr_Dijkstra(
                        'select id, source, target, cost, reverse_cost from edges order by id',
                        %d, %s, %b);
                """.formatted(start_vid, endVids, directed);
        assertPGRoutingOnSampleData(paths, pgroutingSql);
    }

    private void assertPGRoutingOneToOneSampleData(long start_vid, long end_vid, boolean directed) {
        List<Path> paths = this.sqlgGraph.traversal().E().hasLabel("edges").<Path>call(
                        SqlgPGRoutingFactory.NAME,
                        Map.of(
                                SqlgPGRoutingFactory.Params.FUNCTION, SqlgPGRoutingFactory.pgr_dijkstra,
                                SqlgPGRoutingFactory.Params.START_VID, start_vid,
                                SqlgPGRoutingFactory.Params.END_VID, end_vid,
                                SqlgPGRoutingFactory.Params.DIRECTED, directed
                        ))
                .toList();
        String pgroutingSql = """
                SELECT * FROM pgr_Dijkstra(
                        'select id, source, target, cost, reverse_cost from edges order by id',
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

    private void loadWikiGraph() {

//        INSERT INTO wiki (source, target, cost) VALUES
//        (1, 2, 7),  (1, 3, 9), (1, 6, 14),
//        (2, 3, 10), (2, 4, 15),
//        (3, 6, 2),  (3, 4, 11),
//        (4, 5, 6),
//        (5, 6, 9);

        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("wiki", new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
                    put("_index", PropertyDefinition.of(PropertyType.INTEGER, Multiplicity.of(1, 1)));
                }});
        EdgeLabel edgeLabel = vertexLabel.ensureEdgeLabelExist("connects", vertexLabel,
                new LinkedHashMap<>() {{
                    put("source", PropertyDefinition.of(PropertyType.INTEGER));
                    put("target", PropertyDefinition.of(PropertyType.INTEGER));
                    put("cost", PropertyDefinition.of(PropertyType.DOUBLE, Multiplicity.of(1, 1), "'-1'"));
                    put("reverse_cost", PropertyDefinition.of(PropertyType.DOUBLE, Multiplicity.of(1, 1), "'-1'"));
                }}
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().lock();
        Map<Integer, Vertex> vertexMap = new HashMap<>();
        for (Integer i : List.of(1, 2, 3, 4, 5, 6)) {
            vertexMap.put(i, this.sqlgGraph.addVertex(T.label, "wiki", "name", String.valueOf(i), "_index", i));
        }

        Vertex _1 = vertexMap.get(1);
        Vertex _2 = vertexMap.get(2);
        Vertex _3 = vertexMap.get(3);
        Vertex _4 = vertexMap.get(4);
        Vertex _5 = vertexMap.get(5);
        Vertex _6 = vertexMap.get(6);

        _1.addEdge("connects", _2, "source", 1, "target", 2, "cost", 7D);
        _1.addEdge("connects", _3, "source", 1, "target", 3, "cost", 9D);
        _1.addEdge("connects", _6, "source", 1, "target", 6, "cost", 14D);

        _2.addEdge("connects", _3, "source", 2, "target", 3, "cost", 10D);
        _2.addEdge("connects", _4, "source", 2, "target", 4, "cost", 15D);

        _3.addEdge("connects", _6, "source", 3, "target", 6, "cost", 2D);
        _3.addEdge("connects", _4, "source", 3, "target", 4, "cost", 11D);

        _4.addEdge("connects", _5, "source", 4, "target", 5, "cost", 6D);
        _5.addEdge("connects", _6, "source", 5, "target", 6, "cost", 9D);

        this.sqlgGraph.tx().commit();

    }

    private void loadPGRoutingSampleData() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("vertices", new LinkedHashMap<>() {{
                    put("in_edges", PropertyDefinition.of(PropertyType.LONG_ARRAY));
                    put("out_edges", PropertyDefinition.of(PropertyType.LONG_ARRAY));
                    put("x", PropertyDefinition.of(PropertyType.DOUBLE));
                    put("y", PropertyDefinition.of(PropertyType.DOUBLE));
                    put("geom", PropertyDefinition.of(PropertyType.POINT));
                }});
        vertexLabel.ensureEdgeLabelExist("edges", vertexLabel, new LinkedHashMap<>() {{
            put("source", PropertyDefinition.of(PropertyType.LONG));
            put("target", PropertyDefinition.of(PropertyType.LONG));
            put("cost", PropertyDefinition.of(PropertyType.DOUBLE, Multiplicity.of(1, 1), "'-1'"));
            put("reverse_cost", PropertyDefinition.of(PropertyType.DOUBLE, Multiplicity.of(1, 1), "'-1'"));
            put("capacity", PropertyDefinition.of(PropertyType.LONG));
            put("reverse_capacity", PropertyDefinition.of(PropertyType.LONG));
            put("x1", PropertyDefinition.of(PropertyType.DOUBLE));
            put("y1", PropertyDefinition.of(PropertyType.DOUBLE));
            put("x2", PropertyDefinition.of(PropertyType.DOUBLE));
            put("y2", PropertyDefinition.of(PropertyType.DOUBLE));
            put("geom", PropertyDefinition.of(PropertyType.LINESTRING));
        }});
        this.sqlgGraph.tx().commit();

        Vertex v1 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", null, "out_edges", new Long[]{6L}, "x", 0D, "y", 2D, "geom", new Point(0D, 2D));
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", null, "out_edges", new Long[]{17L}, "x", 0.5D, "y", 3.5D, "geom", new Point(0.5D, 3.5D));
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", new Long[]{6L}, "out_edges", new Long[]{7L}, "x", 1D, "y", 2D, "geom", new Point(1D, 2D));
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", new Long[]{17L}, "out_edges", null, "x", 1.999999999999D, "y", 3.5D, "geom", new Point(1.999999999999D, 3.5D));
        Vertex v5 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", null, "out_edges", new Long[]{1L}, "x", 2D, "y", 0D, "geom", new Point(2D, 0D));
        Vertex v6 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", new Long[]{1L}, "out_edges", new Long[]{2L, 4L}, "x", 2D, "y", 1D, "geom", new Point(2D, 1D));
        Vertex v7 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", new Long[]{4L, 7L}, "out_edges", new Long[]{8L, 10L}, "x", 2D, "y", 2D, "geom", new Point(2D, 2D));
        Vertex v8 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", new Long[]{10L}, "out_edges", new Long[]{12L, 14L}, "x", 2D, "y", 3D, "geom", new Point(2D, 3D));
        Vertex v9 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", new Long[]{14L}, "out_edges", null, "x", 2D, "y", 4D, "geom", new Point(2D, 4D));
        Vertex v10 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", new Long[]{2L}, "out_edges", new Long[]{3L, 5L}, "x", 3D, "y", 1D, "geom", new Point(3D, 1D));
        Vertex v11 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", new Long[]{5L, 8L}, "out_edges", new Long[]{9L, 11L}, "x", 3D, "y", 2D, "geom", new Point(3D, 2D));
        Vertex v12 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", new Long[]{11L, 12L}, "out_edges", new Long[]{13L}, "x", 3D, "y", 3D, "geom", new Point(3D, 3D));
        Vertex v13 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", null, "out_edges", new Long[]{18L}, "x", 3.5D, "y", 2.3D, "geom", new Point(3.5D, 2.3D));
        Vertex v14 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", new Long[]{18L}, "out_edges", null, "x", 3.5D, "y", 4D, "geom", new Point(3.5D, 4D));
        Vertex v15 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", new Long[]{3L}, "out_edges", new Long[]{16L}, "x", 4D, "y", 1D, "geom", new Point(4D, 1D));
        Vertex v16 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", new Long[]{9L, 16L}, "out_edges", new Long[]{15L}, "x", 4D, "y", 2D, "geom", new Point(4D, 2D));
        Vertex v17 = this.sqlgGraph.addVertex(T.label, "vertices", "in_edges", new Long[]{13L, 15L}, "out_edges", null, "x", 4D, "y", 3D, "geom", new Point(4D, 3D));

        Edge e1 = v6.addEdge("edges", v5, "source", 5L, "target", 6L, "cost", 1D, "reverse_cost", 1D, "capacity", 80L, "reverse_capacity", 130L, "x1", 2D, "y1", 0D, "x2", 2D, "y2", 1D, "geom", new LineString(new Point[]{v5.value("geom"), v6.value("geom")}));
        Edge e2 = v10.addEdge("edges", v6, "source", 6L, "target", 10L, "cost", -1D, "reverse_cost", 1D, "capacity", -1L, "reverse_capacity", 100L, "x1", 2D, "y1", 1D, "x2", 3D, "y2", 1D, "geom", new LineString(new Point[]{v6.value("geom"), v10.value("geom")}));
        Edge e3 = v15.addEdge("edges", v10, "source", 10L, "target", 15L, "cost", -1D, "reverse_cost", 1D, "capacity", -1L, "reverse_capacity", 130L, "x1", 3D, "y1", 1D, "x2", 4D, "y2", 1D, "geom", new LineString(new Point[]{v10.value("geom"), v15.value("geom")}));
        Edge e4 = v7.addEdge("edges", v6, "source", 6L, "target", 7L, "cost", 1D, "reverse_cost", 1D, "capacity", 100L, "reverse_capacity", 50L, "x1", 2D, "y1", 1D, "x2", 2D, "y2", 2D, "geom", new LineString(new Point[]{v6.value("geom"), v7.value("geom")}));
        Edge e5 = v11.addEdge("edges", v10, "source", 10L, "target", 11L, "cost", 1D, "reverse_cost", -1D, "capacity", 130L, "reverse_capacity", -1L, "x1", 3D, "y1", 1D, "x2", 3D, "y2", 2D, "geom", new LineString(new Point[]{v10.value("geom"), v11.value("geom")}));
        Edge e6 = v3.addEdge("edges", v1, "source", 1L, "target", 3L, "cost", 1D, "reverse_cost", 1D, "capacity", 50L, "reverse_capacity", 100L, "x1", 0D, "y1", 2D, "x2", 1D, "y2", 2D, "geom", new LineString(new Point[]{v1.value("geom"), v3.value("geom")}));
        Edge e7 = v7.addEdge("edges", v3, "source", 3L, "target", 7L, "cost", 1D, "reverse_cost", 1D, "capacity", 50L, "reverse_capacity", 130L, "x1", 1D, "y1", 2D, "x2", 2D, "y2", 2D, "geom", new LineString(new Point[]{v3.value("geom"), v7.value("geom")}));
        Edge e8 = v11.addEdge("edges", v7, "source", 7L, "target", 11L, "cost", 1D, "reverse_cost", 1D, "capacity", 100L, "reverse_capacity", 130L, "x1", 2D, "y1", 2D, "x2", 3D, "y2", 2D, "geom", new LineString(new Point[]{v7.value("geom"), v11.value("geom")}));
        Edge e9 = v16.addEdge("edges", v11, "source", 11L, "target", 16L, "cost", 1D, "reverse_cost", 1D, "capacity", 130L, "reverse_capacity", 80L, "x1", 3D, "y1", 2D, "x2", 4D, "y2", 2D, "geom", new LineString(new Point[]{v11.value("geom"), v16.value("geom")}));
        Edge e10 = v8.addEdge("edges", v7, "source", 7L, "target", 8L, "cost", 1D, "reverse_cost", 1D, "capacity", 130L, "reverse_capacity", 50L, "x1", 2D, "y1", 2D, "x2", 2D, "y2", 3D, "geom", new LineString(new Point[]{v7.value("geom"), v8.value("geom")}));
        Edge e11 = v12.addEdge("edges", v11, "source", 11L, "target", 12L, "cost", 1D, "reverse_cost", -1D, "capacity", 130L, "reverse_capacity", -1L, "x1", 3D, "y1", 2D, "x2", 3D, "y2", 3D, "geom", new LineString(new Point[]{v11.value("geom"), v12.value("geom")}));
        Edge e12 = v12.addEdge("edges", v8, "source", 8L, "target", 12L, "cost", 1D, "reverse_cost", -1D, "capacity", 100L, "reverse_capacity", -1L, "x1", 2D, "y1", 3D, "x2", 3D, "y2", 3D, "geom", new LineString(new Point[]{v8.value("geom"), v12.value("geom")}));
        Edge e13 = v17.addEdge("edges", v12, "source", 12L, "target", 17L, "cost", 1D, "reverse_cost", -1D, "capacity", 100L, "reverse_capacity", -1L, "x1", 3D, "y1", 3D, "x2", 4D, "y2", 3D, "geom", new LineString(new Point[]{v12.value("geom"), v17.value("geom")}));
        Edge e14 = v9.addEdge("edges", v8, "source", 8L, "target", 9L, "cost", 1D, "reverse_cost", 1D, "capacity", 80L, "reverse_capacity", 130L, "x1", 2D, "y1", 3D, "x2", 2D, "y2", 4D, "geom", new LineString(new Point[]{v8.value("geom"), v9.value("geom")}));
        Edge e15 = v17.addEdge("edges", v16, "source", 16L, "target", 17L, "cost", 1D, "reverse_cost", 1D, "capacity", 80L, "reverse_capacity", 50L, "x1", 4D, "y1", 2D, "x2", 4D, "y2", 3D, "geom", new LineString(new Point[]{v16.value("geom"), v17.value("geom")}));
        Edge e16 = v16.addEdge("edges", v15, "source", 15L, "target", 16L, "cost", 1D, "reverse_cost", 1D, "capacity", 80L, "reverse_capacity", 80L, "x1", 4D, "y1", 1D, "x2", 4D, "y2", 2D, "geom", new LineString(new Point[]{v15.value("geom"), v16.value("geom")}));
        Edge e17 = v4.addEdge("edges", v2, "source", 2L, "target", 4L, "cost", 1D, "reverse_cost", 1D, "capacity", 130L, "reverse_capacity", 100L, "x1", 0.5D, "y1", 3.5D, "x2", 1.999999999999D, "y2", 3.5D, "geom", new LineString(new Point[]{v2.value("geom"), v4.value("geom")}));
        Edge e18 = v14.addEdge("edges", v13, "source", 13L, "target", 14L, "cost", 1D, "reverse_cost", 1D, "capacity", 50L, "reverse_capacity", 130L, "x1", 3.5D, "y1", 2.3D, "x2", 3.5D, "y2", 4D, "geom", new LineString(new Point[]{v13.value("geom"), v14.value("geom")}));

        this.sqlgGraph.tx().commit();
    }


}
