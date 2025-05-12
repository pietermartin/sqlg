package org.umlg.sqlg.test.pgvector;

import com.pgvector.PGvector;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.predicate.PGVectorOrderByComparator;
import org.umlg.sqlg.predicate.PGVectorPredicate;
import org.umlg.sqlg.services.SqlgPGVectorFactory;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TestPgVector extends BaseTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(TestPgVector.class);

//    @Test
//    public void testPgVector() {
//        this.sqlgGraph.getTopology().getPublicSchema()
//                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
//                    put("embedding", PropertyDefinition.of(PropertyType.pgvector(3)));
//                }});
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
//        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
//        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
//        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
//        Assert.assertEquals(4, pgvectors.size());
//        for (Vertex pgvector : pgvectors) {
//            PGvector embedding = pgvector.value("embedding");
//            Assert.assertArrayEquals(new float[]{1, 2, 3}, embedding.toArray(), 0F);
//        }
//    }
//
//    @Test
//    public void testUpdatePgVector() {
//        this.sqlgGraph.getTopology().getPublicSchema()
//                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
//                    put("embedding", PropertyDefinition.of(PropertyType.pgvector(3)));
//                }});
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
//        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
//        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
//        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
//        Assert.assertEquals(4, pgvectors.size());
//        for (Vertex pgvector : pgvectors) {
//            PGvector embedding = pgvector.value("embedding");
//            Assert.assertArrayEquals(new float[]{1, 2, 3}, embedding.toArray(), 0F);
//            pgvector.property("embedding", new PGvector(new float[]{3, 2, 1}));
//        }
//        this.sqlgGraph.tx().commit();
//
//        pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
//        Assert.assertEquals(4, pgvectors.size());
//        for (Vertex pgvector : pgvectors) {
//            PGvector embedding = pgvector.value("embedding");
//            Assert.assertArrayEquals(new float[]{3, 2, 1}, embedding.toArray(), 0F);
//        }
//    }
//
//    @Test
//    public void testPgVectorNormalBulkMode() {
//        this.sqlgGraph.getTopology().getPublicSchema()
//                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
//                    put("embedding", PropertyDefinition.of(PropertyType.pgvector(3)));
//                }});
//        this.sqlgGraph.tx().commit();
//
//        this.sqlgGraph.tx().normalBatchModeOn();
//        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
//        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
//        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
//        this.sqlgGraph.addVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
//        Assert.assertEquals(4, pgvectors.size());
//        for (Vertex pgvector : pgvectors) {
//            PGvector embedding = pgvector.value("embedding");
//            Assert.assertArrayEquals(new float[]{1, 2, 3}, embedding.toArray(), 0F);
//        }
//    }
//
//    @Test
//    public void testPgVectorStreamingBulkMode() {
//        this.sqlgGraph.getTopology().getPublicSchema()
//                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
//                    put("embedding", PropertyDefinition.of(PropertyType.pgvector(3)));
//                }});
//        this.sqlgGraph.tx().commit();
//
//        this.sqlgGraph.tx().streamingBatchModeOn();
//        this.sqlgGraph.streamVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
//        this.sqlgGraph.streamVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
//        this.sqlgGraph.streamVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
//        this.sqlgGraph.streamVertex(T.label, "PGVector", "embedding", new PGvector(new float[]{1, 2, 3}));
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
//        Assert.assertEquals(4, pgvectors.size());
//        for (Vertex pgvector : pgvectors) {
//            PGvector embedding = pgvector.value("embedding");
//            Assert.assertArrayEquals(new float[]{1, 2, 3}, embedding.toArray(), 0F);
//        }
//    }

    @Test
    public void testPgVectorQueryL2DistancePredicate() {
        int dimension = 100;
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                    put("embedding", PropertyDefinition.of(PropertyType.pgvector(dimension)));
                }});
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 100; i++) {
            float[] floats = new float[dimension];
            for (int j = 0; j < dimension; j++) {
                floats[j] = new Random().nextFloat();
            }
            this.sqlgGraph.addVertex(T.label, "PGVector", "name", "a", "embedding", new PGvector(floats));
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
        Assert.assertEquals(100, pgvectors.size());

        for (int k = 0; k < 100; k++) {
            float[] toSearchFor = new float[dimension];
            StringBuilder floatToSearchForAsLiteral = new StringBuilder("'[");
            for (int j = 0; j < dimension; j++) {
                toSearchFor[j] = new Random().nextFloat();
                floatToSearchForAsLiteral.append(toSearchFor[j]);
                if (j < dimension - 1) {
                    floatToSearchForAsLiteral.append(",");
                }
            }
            floatToSearchForAsLiteral.append("]'");

            List<Vertex> nearestNeighbours = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                    .has("embedding", PGVectorPredicate.l2Distance(toSearchFor, 5))
                    .toList();
            String sql = """
                    SELECT
                    	"public"."V_PGVector"."ID" AS "alias1",
                    	"public"."V_PGVector"."name" AS "alias2",
                    	"public"."V_PGVector"."embedding" AS "alias3"
                    FROM
                    	"public"."V_PGVector"
                    WHERE "public"."V_PGVector"."embedding" <-> %s < %d
                    """.formatted(floatToSearchForAsLiteral, 5);
            assertPgVector(nearestNeighbours, sql);
        }
    }

    @Test
    public void testPgVectorQueryL2Distance() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                    put("embedding", PropertyDefinition.of(PropertyType.pgvector(100)));
                }});
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 100; i++) {
            float[] floats = new float[100];
            for (int j = 0; j < 100; j++) {
                floats[j] = new Random().nextFloat();
            }
            this.sqlgGraph.addVertex(T.label, "PGVector", "name", "a" + i, "embedding", new PGvector(floats));
        }
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                .<Vertex>call(
                        SqlgPGVectorFactory.NAME,
                        Map.of(
                                SqlgPGVectorFactory.Params.FUNCTION, SqlgPGVectorFactory.l2distance,
                                SqlgPGVectorFactory.Params.SOURCE, "embedding",
                                SqlgPGVectorFactory.Params.TARGET, new float[]{1L, 2L, 3L}
                        )
                )
                .toList();
        for (Vertex vertex : vertices) {
            double l2distance = vertex.value(Graph.Hidden.hide("l2distance"));
        }

        List<Map<Object, Object>> maps = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                .<Vertex>call(
                        SqlgPGVectorFactory.NAME,
                        Map.of(
                                SqlgPGVectorFactory.Params.FUNCTION, SqlgPGVectorFactory.l2distance,
                                SqlgPGVectorFactory.Params.SOURCE, "embedding",
                                SqlgPGVectorFactory.Params.TARGET, new float[]{1L, 2L, 3L}
                        )
                )
                .elementMap("name", Graph.Hidden.hide("l2distance"))
                .toList();
        for (Map<Object, Object> map : maps) {
            String name = (String) map.get("name");
            Double l2Distance = (Double) map.get(Graph.Hidden.hide("l2distance"));
        }

    }

    @Test
    public void testPgVectorQueryL2DistanceOrderBy() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                    put("embedding", PropertyDefinition.of(PropertyType.pgvector(100)));
                }});
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 100; i++) {
            float[] floats = new float[100];
            for (int j = 0; j < 100; j++) {
                floats[j] = new Random().nextFloat();
            }
            this.sqlgGraph.addVertex(T.label, "PGVector", "name", "a", "embedding", new PGvector(floats));
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
        Assert.assertEquals(100, pgvectors.size());

        for (int k = 0; k < 100; k++) {
            float[] toSearchFor = new float[100];
            StringBuilder floatToSearchForAsLiteral = new StringBuilder("'[");
            for (int j = 0; j < 100; j++) {
                toSearchFor[j] = new Random().nextFloat();
                floatToSearchForAsLiteral.append(toSearchFor[j]);
                if (j < 99) {
                    floatToSearchForAsLiteral.append(",");
                }
            }
            floatToSearchForAsLiteral.append("]'");

            List<Vertex> nearestNeighbours = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                    .order().by("embedding", new PGVectorOrderByComparator(toSearchFor))
                    .limit(5)
                    .toList();
            String sql = """
                    SELECT
                    	"public"."V_PGVector"."ID" AS "alias1",
                    	"public"."V_PGVector"."name" AS "alias2",
                    	"public"."V_PGVector"."embedding" AS "alias3"
                    FROM
                    	"public"."V_PGVector"
                    ORDER BY
                    	 "public"."V_PGVector"."embedding" <-> %s
                    LIMIT 5 OFFSET 0
                    """.formatted(floatToSearchForAsLiteral);
            assertPgVector(nearestNeighbours, sql);


        }
    }

    private void assertPgVector(List<Vertex> nearestNeighbours, String sql) {
        Connection connection = this.sqlgGraph.tx().getConnection();
        try (Statement s = connection.createStatement()) {
            ResultSet rs = s.executeQuery(sql);
            Map<Long, Pair<String, float[]>> expected = new HashMap<>();
            while (rs.next()) {
                long id = rs.getLong("alias1");
                String name = rs.getString("alias2");
                PGvector pgvector = rs.getObject("alias3", PGvector.class);
                float[] vector = pgvector.toArray();
                expected.put(id, Pair.of(name, vector));
            }
            for (Vertex nearestNeighbour : nearestNeighbours) {
                long id = ((RecordId) nearestNeighbour.id()).sequenceId();
                PGvector pGvector = nearestNeighbour.value("embedding");
                float[] vector = pGvector.toArray();
                String name = nearestNeighbour.value("name");

                Assert.assertTrue(expected.containsKey(id));

                Pair<String, float[]> pair = expected.remove(id);
                Assert.assertEquals(name, pair.getKey());
                Assert.assertArrayEquals(vector, pair.getValue(), 0F);
            }
            Assert.assertTrue(expected.isEmpty());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
