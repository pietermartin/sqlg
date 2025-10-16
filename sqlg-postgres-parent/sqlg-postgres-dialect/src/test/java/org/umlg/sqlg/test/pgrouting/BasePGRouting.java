package org.umlg.sqlg.test.pgrouting;

import net.postgis.jdbc.geometry.LineString;
import net.postgis.jdbc.geometry.Point;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.umlg.sqlg.services.SqlgPGRoutingFactory;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BasePGRouting extends BaseTest {

    @Before
    public void registerServices() throws Exception {
        super.before();
        sqlgGraph.getServiceRegistry().registerService(new SqlgPGRoutingFactory(sqlgGraph));
    }

    protected void loadPGRoutingSampleData() {
        loadPGRoutingSampleData(this.sqlgGraph.getTopology().getPublicSchema());
    }
    protected void loadPGRoutingSampleData(Schema schema) {
        VertexLabel vertexLabel = schema
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

        String vertexLabelName = schema.getName() + ".vertices";
        Vertex v1 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", null, "out_edges", new Long[]{6L}, "x", 0D, "y", 2D, "geom", new Point(0D, 2D));
        Vertex v2 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", null, "out_edges", new Long[]{17L}, "x", 0.5D, "y", 3.5D, "geom", new Point(0.5D, 3.5D));
        Vertex v3 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", new Long[]{6L}, "out_edges", new Long[]{7L}, "x", 1D, "y", 2D, "geom", new Point(1D, 2D));
        Vertex v4 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", new Long[]{17L}, "out_edges", null, "x", 1.999999999999D, "y", 3.5D, "geom", new Point(1.999999999999D, 3.5D));
        Vertex v5 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", null, "out_edges", new Long[]{1L}, "x", 2D, "y", 0D, "geom", new Point(2D, 0D));
        Vertex v6 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", new Long[]{1L}, "out_edges", new Long[]{2L, 4L}, "x", 2D, "y", 1D, "geom", new Point(2D, 1D));
        Vertex v7 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", new Long[]{4L, 7L}, "out_edges", new Long[]{8L, 10L}, "x", 2D, "y", 2D, "geom", new Point(2D, 2D));
        Vertex v8 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", new Long[]{10L}, "out_edges", new Long[]{12L, 14L}, "x", 2D, "y", 3D, "geom", new Point(2D, 3D));
        Vertex v9 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", new Long[]{14L}, "out_edges", null, "x", 2D, "y", 4D, "geom", new Point(2D, 4D));
        Vertex v10 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", new Long[]{2L}, "out_edges", new Long[]{3L, 5L}, "x", 3D, "y", 1D, "geom", new Point(3D, 1D));
        Vertex v11 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", new Long[]{5L, 8L}, "out_edges", new Long[]{9L, 11L}, "x", 3D, "y", 2D, "geom", new Point(3D, 2D));
        Vertex v12 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", new Long[]{11L, 12L}, "out_edges", new Long[]{13L}, "x", 3D, "y", 3D, "geom", new Point(3D, 3D));
        Vertex v13 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", null, "out_edges", new Long[]{18L}, "x", 3.5D, "y", 2.3D, "geom", new Point(3.5D, 2.3D));
        Vertex v14 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", new Long[]{18L}, "out_edges", null, "x", 3.5D, "y", 4D, "geom", new Point(3.5D, 4D));
        Vertex v15 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", new Long[]{3L}, "out_edges", new Long[]{16L}, "x", 4D, "y", 1D, "geom", new Point(4D, 1D));
        Vertex v16 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", new Long[]{9L, 16L}, "out_edges", new Long[]{15L}, "x", 4D, "y", 2D, "geom", new Point(4D, 2D));
        Vertex v17 = this.sqlgGraph.addVertex(T.label, vertexLabelName, "in_edges", new Long[]{13L, 15L}, "out_edges", null, "x", 4D, "y", 3D, "geom", new Point(4D, 3D));

        String edgeLabelName = "edges";
        Edge e1 = v6.addEdge(edgeLabelName, v5, "source", 5L, "target", 6L, "cost", 1D, "reverse_cost", 1D, "capacity", 80L, "reverse_capacity", 130L, "x1", 2D, "y1", 0D, "x2", 2D, "y2", 1D, "geom", new LineString(new Point[]{v5.value("geom"), v6.value("geom")}));
        Edge e2 = v10.addEdge(edgeLabelName, v6, "source", 6L, "target", 10L, "cost", -1D, "reverse_cost", 1D, "capacity", -1L, "reverse_capacity", 100L, "x1", 2D, "y1", 1D, "x2", 3D, "y2", 1D, "geom", new LineString(new Point[]{v6.value("geom"), v10.value("geom")}));
        Edge e3 = v15.addEdge(edgeLabelName, v10, "source", 10L, "target", 15L, "cost", -1D, "reverse_cost", 1D, "capacity", -1L, "reverse_capacity", 130L, "x1", 3D, "y1", 1D, "x2", 4D, "y2", 1D, "geom", new LineString(new Point[]{v10.value("geom"), v15.value("geom")}));
        Edge e4 = v7.addEdge(edgeLabelName, v6, "source", 6L, "target", 7L, "cost", 1D, "reverse_cost", 1D, "capacity", 100L, "reverse_capacity", 50L, "x1", 2D, "y1", 1D, "x2", 2D, "y2", 2D, "geom", new LineString(new Point[]{v6.value("geom"), v7.value("geom")}));
        Edge e5 = v11.addEdge(edgeLabelName, v10, "source", 10L, "target", 11L, "cost", 1D, "reverse_cost", -1D, "capacity", 130L, "reverse_capacity", -1L, "x1", 3D, "y1", 1D, "x2", 3D, "y2", 2D, "geom", new LineString(new Point[]{v10.value("geom"), v11.value("geom")}));
        Edge e6 = v3.addEdge(edgeLabelName, v1, "source", 1L, "target", 3L, "cost", 1D, "reverse_cost", 1D, "capacity", 50L, "reverse_capacity", 100L, "x1", 0D, "y1", 2D, "x2", 1D, "y2", 2D, "geom", new LineString(new Point[]{v1.value("geom"), v3.value("geom")}));
        Edge e7 = v7.addEdge(edgeLabelName, v3, "source", 3L, "target", 7L, "cost", 1D, "reverse_cost", 1D, "capacity", 50L, "reverse_capacity", 130L, "x1", 1D, "y1", 2D, "x2", 2D, "y2", 2D, "geom", new LineString(new Point[]{v3.value("geom"), v7.value("geom")}));
        Edge e8 = v11.addEdge(edgeLabelName, v7, "source", 7L, "target", 11L, "cost", 1D, "reverse_cost", 1D, "capacity", 100L, "reverse_capacity", 130L, "x1", 2D, "y1", 2D, "x2", 3D, "y2", 2D, "geom", new LineString(new Point[]{v7.value("geom"), v11.value("geom")}));
        Edge e9 = v16.addEdge(edgeLabelName, v11, "source", 11L, "target", 16L, "cost", 1D, "reverse_cost", 1D, "capacity", 130L, "reverse_capacity", 80L, "x1", 3D, "y1", 2D, "x2", 4D, "y2", 2D, "geom", new LineString(new Point[]{v11.value("geom"), v16.value("geom")}));
        Edge e10 = v8.addEdge(edgeLabelName, v7, "source", 7L, "target", 8L, "cost", 1D, "reverse_cost", 1D, "capacity", 130L, "reverse_capacity", 50L, "x1", 2D, "y1", 2D, "x2", 2D, "y2", 3D, "geom", new LineString(new Point[]{v7.value("geom"), v8.value("geom")}));
        Edge e11 = v12.addEdge(edgeLabelName, v11, "source", 11L, "target", 12L, "cost", 1D, "reverse_cost", -1D, "capacity", 130L, "reverse_capacity", -1L, "x1", 3D, "y1", 2D, "x2", 3D, "y2", 3D, "geom", new LineString(new Point[]{v11.value("geom"), v12.value("geom")}));
        Edge e12 = v12.addEdge(edgeLabelName, v8, "source", 8L, "target", 12L, "cost", 1D, "reverse_cost", -1D, "capacity", 100L, "reverse_capacity", -1L, "x1", 2D, "y1", 3D, "x2", 3D, "y2", 3D, "geom", new LineString(new Point[]{v8.value("geom"), v12.value("geom")}));
        Edge e13 = v17.addEdge(edgeLabelName, v12, "source", 12L, "target", 17L, "cost", 1D, "reverse_cost", -1D, "capacity", 100L, "reverse_capacity", -1L, "x1", 3D, "y1", 3D, "x2", 4D, "y2", 3D, "geom", new LineString(new Point[]{v12.value("geom"), v17.value("geom")}));
        Edge e14 = v9.addEdge(edgeLabelName, v8, "source", 8L, "target", 9L, "cost", 1D, "reverse_cost", 1D, "capacity", 80L, "reverse_capacity", 130L, "x1", 2D, "y1", 3D, "x2", 2D, "y2", 4D, "geom", new LineString(new Point[]{v8.value("geom"), v9.value("geom")}));
        Edge e15 = v17.addEdge(edgeLabelName, v16, "source", 16L, "target", 17L, "cost", 1D, "reverse_cost", 1D, "capacity", 80L, "reverse_capacity", 50L, "x1", 4D, "y1", 2D, "x2", 4D, "y2", 3D, "geom", new LineString(new Point[]{v16.value("geom"), v17.value("geom")}));
        Edge e16 = v16.addEdge(edgeLabelName, v15, "source", 15L, "target", 16L, "cost", 1D, "reverse_cost", 1D, "capacity", 80L, "reverse_capacity", 80L, "x1", 4D, "y1", 1D, "x2", 4D, "y2", 2D, "geom", new LineString(new Point[]{v15.value("geom"), v16.value("geom")}));
        Edge e17 = v4.addEdge(edgeLabelName, v2, "source", 2L, "target", 4L, "cost", 1D, "reverse_cost", 1D, "capacity", 130L, "reverse_capacity", 100L, "x1", 0.5D, "y1", 3.5D, "x2", 1.999999999999D, "y2", 3.5D, "geom", new LineString(new Point[]{v2.value("geom"), v4.value("geom")}));
        Edge e18 = v14.addEdge(edgeLabelName, v13, "source", 13L, "target", 14L, "cost", 1D, "reverse_cost", 1D, "capacity", 50L, "reverse_capacity", 130L, "x1", 3.5D, "y1", 2.3D, "x2", 3.5D, "y2", 4D, "geom", new LineString(new Point[]{v13.value("geom"), v14.value("geom")}));

        this.sqlgGraph.tx().commit();
    }

    protected  void loadWikiGraph() {

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
}
