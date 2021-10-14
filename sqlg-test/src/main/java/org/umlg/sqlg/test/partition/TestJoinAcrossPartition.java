package org.umlg.sqlg.test.partition;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

public class TestJoinAcrossPartition extends BaseTest {

    @Before
    public void before() throws Exception {
        super.before();
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsPartitioning());
    }

    @Test
    public void testJoinHitsPartitions() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel animalTypeVertexLabel = publicSchema.ensureVertexLabelExist(
                "AnimalType",
                new LinkedHashMap<>() {{
                    put("type", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of("type"))
        );
        VertexLabel animalVertexLabel = publicSchema.ensurePartitionedVertexLabelExist(
                "Animal",
                new LinkedHashMap<>() {{
                    put("type", PropertyType.STRING);
                    put("name", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of("type", "name")),
                PartitionType.LIST,
                "\"type\""
        );
        animalVertexLabel.ensureListPartitionExists("dog_type", "'dog'");
        animalVertexLabel.ensureListPartitionExists("cat_type", "'cat'");
        animalVertexLabel.ensureListPartitionExists("mouse_type", "'mouse'");
        EdgeLabel hasEdgeLabel = publicSchema.ensurePartitionedEdgeLabelExistOnInOrOutVertexLabel(
                "has",
                animalTypeVertexLabel,
                animalVertexLabel,
                new LinkedHashMap<>() {{
                    put("uid", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid")),
                PartitionType.LIST,
                animalTypeVertexLabel
        );
        hasEdgeLabel.ensureListPartitionExists("edge_dog_type", "'dog'");
        hasEdgeLabel.ensureListPartitionExists("edge_cat_type", "'cat'");
        hasEdgeLabel.ensureListPartitionExists("edge_mouse_type", "'mouse'");
        this.sqlgGraph.tx().commit();

        Vertex dogType = this.sqlgGraph.addVertex(T.label, "AnimalType", "type", "dog", "name", "dog1");
        Vertex catType = this.sqlgGraph.addVertex(T.label, "AnimalType", "type", "cat", "name", "cat1");
        Vertex mouseType = this.sqlgGraph.addVertex(T.label, "AnimalType", "type", "mouse", "name", "mouse1");
        for (int i = 0; i < 10; i++) {
            Vertex dog = this.sqlgGraph.addVertex(T.label, "Animal", "type", "dog", "name", "dog" + i);
            dogType.addEdge("has", dog, "uid", UUID.randomUUID().toString());
            Vertex cat = this.sqlgGraph.addVertex(T.label, "Animal", "type", "cat", "name", "cat" + i);
            catType.addEdge("has", cat, "uid", UUID.randomUUID().toString());
            Vertex mouse = this.sqlgGraph.addVertex(T.label, "Animal", "type", "mouse", "name", "mouse" + i);
            mouseType.addEdge("has", mouse, "uid", UUID.randomUUID().toString());
        }

        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V()
                .hasLabel("AnimalType")
                .out("has")
                .toList();
        Assert.assertEquals(30, vertices.size());
        vertices = this.sqlgGraph.traversal().V()
                .hasLabel("AnimalType").has("type", "dog")
                .out("has")
                .has("type", "dog")
                .toList();
        Assert.assertEquals(10, vertices.size());

        //Check if the partitions are being used
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            boolean foundEdgeDogType = false;
            boolean foundDogTypePK = false;
            if (this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
                ResultSet rs = statement.executeQuery("explain SELECT\n" +
                        "\t\"public\".\"V_Animal\".\"type\" AS \"alias1\",\n" +
                        "\t\"public\".\"V_Animal\".\"name\" AS \"alias2\"\n" +
                        "FROM\n" +
                        "\t\"public\".\"V_AnimalType\" INNER JOIN\n" +
                        "\t\"public\".\"E_has\" ON \"public\".\"V_AnimalType\".\"type\" = \"public\".\"E_has\".\"public.AnimalType.type__O\" INNER JOIN\n" +
                        "\t\"public\".\"V_Animal\" ON \"public\".\"E_has\".\"public.Animal.type__I\" = \"public\".\"V_Animal\".\"type\" AND \"public\".\"E_has\".\"public.Animal.name__I\" = \"public\".\"V_Animal\".\"name\"\n" +
                        "WHERE\n" +
                        "\t( \"public\".\"V_AnimalType\".\"type\" = 'dog') AND ( \"public\".\"V_Animal\".\"type\" = 'dog')");
                while (rs.next()) {
                    String result = rs.getString(1);
                    foundEdgeDogType = foundEdgeDogType || result.contains("Bitmap Heap Scan on edge_dog_type");
                    foundDogTypePK = foundDogTypePK || result.contains("Index Only Scan using dog_type_pkey");
                }
            }
            Assert.assertTrue(foundEdgeDogType);
            Assert.assertTrue(foundDogTypePK);
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        this.sqlgGraph.tx().rollback();
    }
}
