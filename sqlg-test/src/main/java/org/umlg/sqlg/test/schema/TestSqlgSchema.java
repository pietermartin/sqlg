package org.umlg.sqlg.test.schema;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.strategy.TopologyStrategy;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.time.LocalDateTime;
import java.util.List;

import static org.umlg.sqlg.structure.topology.Topology.*;

/**
 * Created by pieter on 2015/12/09.
 */
public class TestSqlgSchema extends BaseTest {

    @Test
    public void testSqlgSchemaExist() {
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
        Vertex dog = this.sqlgGraph.addVertex(T.label, "Dog", "name", "Snowy");
        person.addEdge("pet", dog, "createdOn", LocalDateTime.now());

        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().count().next(), 0);
        this.sqlgGraph.close();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {

            GraphTraversalSource traversalSource = sqlgGraph1.traversal().withStrategies(
                    TopologyStrategy.build().sqlgSchema().create()
            );
            //Assert the schema
            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .toList();
            Assert.assertEquals(1, schemas.size());
            Assert.assertEquals(sqlgGraph1.getSqlDialect().getPublicSchema(), schemas.get(0).value("name"));

            //Assert the vertex labels
            List<Vertex> vertexLabels = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_VERTEX_LABEL)
                    .toList();
            Assert.assertEquals(2, vertexLabels.size());
            Assert.assertEquals(1, vertexLabels.stream().filter(v -> v.value("name").equals("Person")).count());
            Assert.assertEquals(1, vertexLabels.stream().filter(v -> v.value("name").equals("Dog")).count());

            //Assert the edge labels
            List<Vertex> edgeLabels = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_EDGE_LABEL)
                    .toList();
            Assert.assertEquals(1, edgeLabels.size());
            Assert.assertEquals(1, edgeLabels.stream().filter(v -> v.value("name").equals("pet")).count());

            //Assert the Person's properties
            List<Vertex> vertexLabelPersons = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_VERTEX_LABEL)
                    .has("name", "Person")
                    .toList();
            Assert.assertEquals(1, vertexLabelPersons.size());
            Vertex vertexLabelPerson = vertexLabelPersons.get(0);
            List<Vertex> personProperties = traversalSource.V(vertexLabelPerson).out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE).toList();
            Assert.assertEquals(1, personProperties.size());
            Assert.assertEquals("name", personProperties.get(0).value("name"));
            Assert.assertEquals("STRING", personProperties.get(0).value("type"));

            //Assert the Dog's properties
            List<Vertex> vertexLabelDogs = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_VERTEX_LABEL)
                    .has("name", "Dog")
                    .toList();
            Assert.assertEquals(1, vertexLabelDogs.size());
            Vertex vertexLabelDog = vertexLabelDogs.get(0);
            List<Vertex> dogProperties = traversalSource.V(vertexLabelDog).out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE).toList();
            Assert.assertEquals(1, dogProperties.size());
            Assert.assertEquals("name", dogProperties.get(0).value("name"));
            Assert.assertEquals("STRING", personProperties.get(0).value("type"));

            //Assert the pet edge's properties
            List<Vertex> edgeLabelPets = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_EDGE_LABEL)
                    .has("name", "pet")
                    .toList();
            Assert.assertEquals(1, edgeLabelPets.size());
            Vertex edgeLabelPet = edgeLabelPets.get(0);
            List<Vertex> petProperties = traversalSource.V(edgeLabelPet).out(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE).toList();
            Assert.assertEquals(1, petProperties.size());
            Assert.assertEquals("createdOn", petProperties.get(0).value("name"));
            Assert.assertEquals("LOCALDATETIME", petProperties.get(0).value("type"));

            //assert that the topology edges are also queryable

            List<Edge> edges = traversalSource.V().hasLabel("sqlg_schema.schema").outE().toList();
            Assert.assertEquals(2, edges.size());
            edges = traversalSource.V().hasLabel("sqlg_schema.schema").out().outE("out_edges").toList();
            Assert.assertEquals(1, edges.size());
            edges = traversalSource.V().hasLabel("sqlg_schema.schema").out().outE("in_edges").toList();
            Assert.assertEquals(1, edges.size());
            edges = traversalSource.V().hasLabel("sqlg_schema.schema").out().outE("vertex_property").toList();
            Assert.assertEquals(2, edges.size());
            edges = traversalSource.V().hasLabel("sqlg_schema.schema").out().out("out_edges").outE("edge_property").toList();
            Assert.assertEquals(1, edges.size());

            List<Edge> topologyEdges = traversalSource.E().toList();
            Assert.assertEquals(7, topologyEdges.size());
        }
    }

}
