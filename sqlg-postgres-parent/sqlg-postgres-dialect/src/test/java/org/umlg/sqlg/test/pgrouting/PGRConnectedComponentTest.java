package org.umlg.sqlg.test.pgrouting;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.services.SqlgPGRoutingFactory;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

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
                  'SELECT "ID" as id, source, target, cost, reverse_cost FROM "E_edges"'
                  );
                """;
        assertPGConnectedComponents(result, pgr_drivingDistance);
    }

    @Test
    public void testConnectedComponent() {
        loadPGRoutingSampleData();

        List<Vertex> components = this.sqlgGraph.traversal().E().hasLabel("edges").pgrConnectedComponent().toList();
        for (Vertex v: components) {
            long component = v.value(Graph.Hidden.hide(SqlgPGRoutingFactory.TRAVERSAL_COMPONENT));
            LOGGER.debug("{}: {}", component, v);
        }
        String pgr_connectedComponents = """
                SELECT * FROM pgr_connectedComponents(
                  'SELECT "ID" as id, source, target, cost, reverse_cost FROM "E_edges"'
                  );
                """;
        assertPGConnectedComponents(components, pgr_connectedComponents);
    }

    @Test
    public void testConnectedComponentInSchema() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
        }});
        aVertexLabel.ensureEdgeLabelExist("ab", aVertexLabel, new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("cost", PropertyDefinition.of(PropertyType.DOUBLE));
            put("reverse_cost", PropertyDefinition.of(PropertyType.DOUBLE));
        }});
        this.sqlgGraph.tx().commit();


        Vertex v6 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "6");
        Vertex v7 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "7");
        Vertex v11 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "11");
        Vertex v16 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "16");
        Vertex v15 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "15");
        Vertex v10 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "10");

        String edgeLabelName = "edges";
        Edge e4 = v6.addEdge(edgeLabelName, v7, "source", 6L, "target", 7L, "cost", 1D, "reverse_cost", 1D);
        Edge e8 = v7.addEdge(edgeLabelName, v11, "source", 7L, "target", 11L, "cost", 1D, "reverse_cost", 1D);
        Edge e9 = v11.addEdge(edgeLabelName, v16, "source", 11L, "target", 16L, "cost", 1D, "reverse_cost", 1D);
        Edge e16 = v15.addEdge(edgeLabelName, v16, "source", 15L, "target", 16L, "cost", 1D, "reverse_cost", 1D);
        Edge e3 = v10.addEdge(edgeLabelName, v15, "source", 10L, "target", 15L, "cost", -1D, "reverse_cost", 1D);
        Edge e2 = v6.addEdge(edgeLabelName, v10, "source", 6L, "target", 10L, "cost", -1D, "reverse_cost", 1D);

        this.sqlgGraph.tx().commit();

        List<Vertex> connectedComponents = this.sqlgGraph.traversal().E().hasLabel("A.edges")
                .pgrConnectedComponent()
                .toList();
        Assert.assertEquals(6, connectedComponents.size());

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
