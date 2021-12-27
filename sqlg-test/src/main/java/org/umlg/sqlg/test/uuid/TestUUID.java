package org.umlg.sqlg.test.uuid;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class TestUUID extends BaseTest {

    @Before
    public void before() throws Exception {
        super.before();
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsUUID());
    }

    @Test
    public void testUUID() {
        UUID uuidPerson1 = UUID.randomUUID();
        Vertex vertexA = this.sqlgGraph.addVertex(T.label, "Person", "uuid", uuidPerson1);
        UUID uuidPerson2 = UUID.randomUUID();
        Vertex vertexB = this.sqlgGraph.addVertex(T.label, "Person", "uuid", uuidPerson2);
        UUID uuidEdge = UUID.randomUUID();
        Edge edge = vertexA.addEdge("knows", vertexB, "uuid", uuidEdge);
        vertexA.addEdge("knows", vertexB, "uuid", UUID.randomUUID());
        vertexA.addEdge("knows", vertexB, "uuid", UUID.randomUUID());
        this.sqlgGraph.tx().commit();
        vertexA = this.sqlgGraph.traversal().V(vertexA.id()).next();
        UUID uuidFromDB = vertexA.value("uuid");
        Assert.assertEquals(uuidPerson1, uuidFromDB);
        vertexB = this.sqlgGraph.traversal().V(vertexB.id()).next();
        uuidFromDB = vertexB.value("uuid");
        Assert.assertEquals(uuidPerson2, uuidFromDB);
        edge = this.sqlgGraph.traversal().E(edge.id()).next();
        uuidFromDB = edge.value("uuid");
        Assert.assertEquals(uuidEdge, uuidFromDB);

        List<Vertex> persons = this.sqlgGraph.traversal().V().hasLabel("Person").has("uuid", uuidPerson1).toList();
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(uuidPerson1, persons.get(0).value("uuid"));
        persons = this.sqlgGraph.traversal().V().hasLabel("Person").has("uuid", uuidPerson2).toList();
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(uuidPerson2, persons.get(0).value("uuid"));
        List<Edge> edges = this.sqlgGraph.traversal().E().hasLabel("knows").has("uuid", uuidEdge).toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(uuidEdge, edges.get(0).value("uuid"));

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("uuid", uuidPerson1)
                .outE().has("uuid", uuidEdge)
                .otherV()
                .toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testUUIDViaTopologyApi() {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person", new HashMap<>() {{
            put("uuid", PropertyType.UUID);
        }});
        personVertexLabel.ensureEdgeLabelExist("knows", personVertexLabel, new HashMap<>() {{
            put("uuid", PropertyType.UUID);
        }});
        this.sqlgGraph.tx().commit();

        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Person").orElseThrow();
        Optional<PropertyColumn> uuidPropertyColumnOptional = vertexLabel.getProperty("uuid");
        Assert.assertTrue(uuidPropertyColumnOptional.isPresent());
        Assert.assertEquals(PropertyType.UUID, uuidPropertyColumnOptional.get().getPropertyType());

        EdgeLabel edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("knows").orElseThrow();
        uuidPropertyColumnOptional = edgeLabel.getProperty("uuid");
        Assert.assertTrue(uuidPropertyColumnOptional.isPresent());
        Assert.assertEquals(PropertyType.UUID, uuidPropertyColumnOptional.get().getPropertyType());

        UUID uuidPerson1 = UUID.randomUUID();
        Vertex vertexA = this.sqlgGraph.addVertex(T.label, "Person", "uuid", uuidPerson1);
        UUID uuidPerson2 = UUID.randomUUID();
        Vertex vertexB = this.sqlgGraph.addVertex(T.label, "Person", "uuid", uuidPerson2);
        UUID uuidEdge = UUID.randomUUID();
        Edge edge = vertexA.addEdge("knows", vertexB, "uuid", uuidEdge);
        vertexA.addEdge("knows", vertexB, "uuid", UUID.randomUUID());
        vertexA.addEdge("knows", vertexB, "uuid", UUID.randomUUID());
        this.sqlgGraph.tx().commit();
        vertexA = this.sqlgGraph.traversal().V(vertexA.id()).next();
        UUID uuidFromDB = vertexA.value("uuid");
        Assert.assertEquals(uuidPerson1, uuidFromDB);
        vertexB = this.sqlgGraph.traversal().V(vertexB.id()).next();
        uuidFromDB = vertexB.value("uuid");
        Assert.assertEquals(uuidPerson2, uuidFromDB);
        edge = this.sqlgGraph.traversal().E(edge.id()).next();
        uuidFromDB = edge.value("uuid");
        Assert.assertEquals(uuidEdge, uuidFromDB);

        List<Vertex> persons = this.sqlgGraph.traversal().V().hasLabel("Person").has("uuid", uuidPerson1).toList();
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(uuidPerson1, persons.get(0).value("uuid"));
        persons = this.sqlgGraph.traversal().V().hasLabel("Person").has("uuid", uuidPerson2).toList();
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(uuidPerson2, persons.get(0).value("uuid"));
        List<Edge> edges = this.sqlgGraph.traversal().E().hasLabel("knows").has("uuid", uuidEdge).toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(uuidEdge, edges.get(0).value("uuid"));

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("uuid", uuidPerson1)
                .outE().has("uuid", uuidEdge)
                .otherV()
                .toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testUUIDAsIdentifier() {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new HashMap<>() {{
                    put("uuid", PropertyType.UUID);
                }},
                ListOrderedSet.listOrderedSet(List.of("uuid"))
        );
        personVertexLabel.ensureEdgeLabelExist(
                "knows",
                personVertexLabel,
                new HashMap<>() {{
                    put("uuid", PropertyType.UUID);
                }},
                ListOrderedSet.listOrderedSet(List.of("uuid")));
        this.sqlgGraph.tx().commit();

        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "uuid", UUID.randomUUID());
        for (int i = 0; i < 100; i++) {
            Vertex other = this.sqlgGraph.addVertex(T.label, "Person", "uuid", UUID.randomUUID());
            person1.addEdge("knows", other, "uuid", UUID.randomUUID());
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> others = this.sqlgGraph.traversal().V().hasId(person1.id()).out("knows").toList();
        Assert.assertEquals(100, others.size());
        List<Edge> otherEdges = this.sqlgGraph.traversal().V().hasId(person1.id()).outE("knows").toList();
        Assert.assertEquals(100, otherEdges.size());
    }

    @Test
    public void testUUIDViaNormalBatchMode() {
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "uuid", UUID.randomUUID());
        }
        this.sqlgGraph.tx().commit();
        List<UUID> uuids = this.sqlgGraph.traversal().V().hasLabel("A").<UUID>values("uuid").toList();
        Assert.assertEquals(100, uuids.size());
    }

    @Test
    public void testUUIDViaStreamingBatchMode() {
        Assume.assumeTrue(isPostgres());
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex(T.label, "A", "uuid", UUID.randomUUID());
        }
        this.sqlgGraph.tx().commit();
        List<UUID> uuids = this.sqlgGraph.traversal().V().hasLabel("A").<UUID>values("uuid").toList();
        Assert.assertEquals(100, uuids.size());
    }
}
