package org.umlg.sqlg.test.services;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.services.SqlgPGRoutingFactory;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SqlgGraphPGRoutingTest extends BaseTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(SqlgGraphPGRoutingTest.class);
    @Before
    public void registerServices() throws Exception {
        Assume.assumeTrue(isPostgres());
        super.before();
        sqlgGraph.getServiceRegistry().registerService(new SqlgPGRoutingFactory(sqlgGraph));
    }

    //    @Test
    public void g_V_call_degree_centrality() {
        Assume.assumeTrue(isPostgres());
        List<Object> result = sqlgGraph.traversal().V().hasLabel("wiki").as("v").call(SqlgPGRoutingFactory.NAME, Map.of()).toList();
        for (Object o : result) {
            System.out.println(o.toString());
        }
    }

//    @Test
    public void g_V_call_dijkstra() {
        Assume.assumeTrue(isPostgres());
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
                        p.get(0) instanceof Vertex && ((Vertex) p.get(0)).value("name").equals("1") && ((Vertex) p.get(0)).value("_index").equals(1) &&
                        p.get(1) instanceof Edge && ((Edge) p.get(1)).value("source").equals(1) && ((Edge) p.get(1)).value("target").equals(3) &&
                        p.get(2) instanceof Vertex && ((Vertex) p.get(2)).value("name").equals("3") && ((Vertex) p.get(2)).value("_index").equals(3) &&
                        p.get(3) instanceof Edge && ((Edge) p.get(3)).value("source").equals(3) && ((Edge) p.get(3)).value("target").equals(6) &&
                        p.get(4) instanceof Vertex && ((Vertex) p.get(4)).value("name").equals("6") && ((Vertex) p.get(4)).value("_index").equals(6) &&
                        p.get(5) instanceof Edge && ((Edge) p.get(5)).value("source").equals(5) && ((Edge) p.get(5)).value("target").equals(6) &&
                        p.get(6) instanceof Vertex && ((Vertex) p.get(6)).value("name").equals("5") && ((Vertex) p.get(6)).value("_index").equals(5)
        );
    }

    @Test
    public void g_V_call_dijkstra_on_sampleData() {
        Assume.assumeTrue(isPostgres());
        loadPGRoutingSampleData();
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
                    put("cost", PropertyDefinition.of(PropertyType.INTEGER));
                }}
        );
        this.sqlgGraph.tx().commit();
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

        _1.addEdge("connects", _2, "source", 1, "target", 2, "cost", 7);
        _1.addEdge("connects", _3, "source", 1, "target", 3, "cost", 9);
        _1.addEdge("connects", _6, "source", 1, "target", 6, "cost", 14);

        _2.addEdge("connects", _3, "source", 2, "target", 3, "cost", 10);
        _2.addEdge("connects", _4, "source", 2, "target", 4, "cost", 15);

        _3.addEdge("connects", _6, "source", 3, "target", 6, "cost", 2);
        _3.addEdge("connects", _4, "source", 3, "target", 4, "cost", 11);

        _4.addEdge("connects", _5, "source", 4, "target", 5, "cost", 6);
        _5.addEdge("connects", _6, "source", 5, "target", 6, "cost", 9);

        this.sqlgGraph.tx().commit();

    }

    private void loadPGRoutingSampleData() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("vertices", new LinkedHashMap<>() {{
                    put("in_edges", PropertyDefinition.of(PropertyType.LONG_ARRAY));
                    put("out_edges", PropertyDefinition.of(PropertyType.LONG_ARRAY));
                    put("x", PropertyDefinition.of(PropertyType.DOUBLE));
                    put("y", PropertyDefinition.of(PropertyType.DOUBLE));
                    put("geom", PropertyDefinition.of(PropertyType.GEOGRAPHY_POINT));
                }});
        vertexLabel.ensureEdgeLabelExist("edges", vertexLabel, new LinkedHashMap<>() {{
            put("source", PropertyDefinition.of(PropertyType.LONG));
            put("target", PropertyDefinition.of(PropertyType.LONG));
            put("cost", PropertyDefinition.of(PropertyType.FLOAT));
            put("reverse_cost", PropertyDefinition.of(PropertyType.FLOAT));
            put("capacity", PropertyDefinition.of(PropertyType.LONG));
            put("reverse_capacity", PropertyDefinition.of(PropertyType.LONG));
            put("x1", PropertyDefinition.of(PropertyType.FLOAT));
            put("y1", PropertyDefinition.of(PropertyType.FLOAT));
            put("x2", PropertyDefinition.of(PropertyType.FLOAT));
            put("y2", PropertyDefinition.of(PropertyType.FLOAT));
            put("geom", PropertyDefinition.of(PropertyType.LINESTRING));
        }});
        this.sqlgGraph.tx().commit();


//        Point johannesburgPoint = new Point(26.2044, 28.0456);
//        Point pretoriaPoint = new Point(25.7461, 28.1881);
//        LineString lineString = new LineString(new Point[] {johannesburgPoint, pretoriaPoint});
//
//        this.sqlgGraph.addVertex(T.label, "vertices", "out_edges", new Long[]{6L}, "x", 0D, "y", 2D, "geom", )

        
    }


}
