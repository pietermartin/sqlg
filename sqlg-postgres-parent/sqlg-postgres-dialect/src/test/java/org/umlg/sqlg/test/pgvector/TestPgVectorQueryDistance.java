package org.umlg.sqlg.test.pgvector;

import com.pgvector.PGbit;
import com.pgvector.PGvector;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TestPgVectorQueryDistance extends BaseTest {

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

        float[] vectorToSearchFor = new float[100];
        for (int i = 0; i < 100; i++) {
            vectorToSearchFor[i] = new Random().nextFloat();
        }

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                .<Vertex>l2distance(
                        "distance",
                        "embedding",
                        vectorToSearchFor
                )
                .toList();
        Map<RecordId, Double> recordIdDistanceMap = new HashMap<>();
        for (Vertex vertex : vertices) {
            double l2distance = vertex.value("distance");
            recordIdDistanceMap.put((RecordId) vertex.id(), l2distance);
        }
        assertPgVector(sqlgGraph, vertices, """
                SELECT
                	"public"."V_PGVector"."ID" AS "alias1",
                	"public"."V_PGVector"."name" AS "alias2",
                	"public"."V_PGVector"."embedding" AS "alias3",
                	("embedding" <-> '[0.013207972,0.5191829,0.016266346,0.36312515,0.09715974,0.7004016,0.8037289,0.17637652,0.75307536,0.86576074,0.5792903,0.14926344,0.6025354,0.7725544,0.6670583,0.4929827,0.75677425,0.14727616,0.87836516,0.75937736,0.7959671,0.4385711,0.60409015,0.31040645,0.8505831,0.3097828,0.85263747,0.2711857,0.034220576,0.0034450293,0.8576316,0.16349697,0.067469716,0.23246193,0.96580803,0.67415786,0.040288746,0.24839157,0.7006174,0.040218353,0.74230176,0.82737064,0.5928458,0.8221752,0.37805742,0.05102992,0.9473907,0.62126136,0.6837731,0.689778,0.17082113,0.52562755,0.564943,0.034926772,0.073277414,0.25918287,0.16011697,0.84725004,0.21584737,0.7118595,0.8870184,0.24177808,0.059740663,0.6781068,0.9303504,0.4947033,0.350994,0.47838026,0.966976,0.55348635,0.6338031,0.44789523,0.9762635,0.36514664,0.854322,0.14553076,0.5975453,0.4605006,1.7422438E-4,0.07356751,0.2692225,0.7938732,0.56638366,0.33757365,0.20897722,0.20917702,0.6014197,0.85098714,0.37343687,0.10486883,0.3122658,0.36180395,0.1056025,0.68098193,0.50820607,0.14893168,0.6463439,0.03368807,0.72256225,0.3791278]') AS "distance"
                FROM
                	"public"."V_PGVector"
                """);

        List<Map<Object, Object>> maps = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                .<Vertex>l2distance(
                        "distance",
                        "embedding",
                        vectorToSearchFor
                )
                .elementMap("name", "distance")
                .toList();
        for (Map<Object, Object> map : maps) {
            RecordId id = (RecordId) map.get(T.id);
            String name = (String) map.get("name");
            Double l2Distance = (Double) map.get("distance");
            Assert.assertEquals(l2Distance, recordIdDistanceMap.remove(id));
        }
        Assert.assertTrue(recordIdDistanceMap.isEmpty());

    }


    @Test
    public void testPgVectorQueryL1Distance() {
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

        float[] vectorToSearchFor = new float[100];
        for (int i = 0; i < 100; i++) {
            vectorToSearchFor[i] = new Random().nextFloat();
        }

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                .<Vertex>l1distance(
                        "distance",
                        "embedding",
                        vectorToSearchFor
                )
                .toList();
        Map<RecordId, Double> recordIdDistanceMap = new HashMap<>();
        for (Vertex vertex : vertices) {
            double l2distance = vertex.value("distance");
            recordIdDistanceMap.put((RecordId) vertex.id(), l2distance);
        }
        assertPgVector(sqlgGraph, vertices, """
                SELECT
                	"public"."V_PGVector"."ID" AS "alias1",
                	"public"."V_PGVector"."name" AS "alias2",
                	"public"."V_PGVector"."embedding" AS "alias3",
                	("embedding" <+> '[0.013207972,0.5191829,0.016266346,0.36312515,0.09715974,0.7004016,0.8037289,0.17637652,0.75307536,0.86576074,0.5792903,0.14926344,0.6025354,0.7725544,0.6670583,0.4929827,0.75677425,0.14727616,0.87836516,0.75937736,0.7959671,0.4385711,0.60409015,0.31040645,0.8505831,0.3097828,0.85263747,0.2711857,0.034220576,0.0034450293,0.8576316,0.16349697,0.067469716,0.23246193,0.96580803,0.67415786,0.040288746,0.24839157,0.7006174,0.040218353,0.74230176,0.82737064,0.5928458,0.8221752,0.37805742,0.05102992,0.9473907,0.62126136,0.6837731,0.689778,0.17082113,0.52562755,0.564943,0.034926772,0.073277414,0.25918287,0.16011697,0.84725004,0.21584737,0.7118595,0.8870184,0.24177808,0.059740663,0.6781068,0.9303504,0.4947033,0.350994,0.47838026,0.966976,0.55348635,0.6338031,0.44789523,0.9762635,0.36514664,0.854322,0.14553076,0.5975453,0.4605006,1.7422438E-4,0.07356751,0.2692225,0.7938732,0.56638366,0.33757365,0.20897722,0.20917702,0.6014197,0.85098714,0.37343687,0.10486883,0.3122658,0.36180395,0.1056025,0.68098193,0.50820607,0.14893168,0.6463439,0.03368807,0.72256225,0.3791278]') AS "distance"
                FROM
                	"public"."V_PGVector"
                """);

        List<Map<Object, Object>> maps = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                .<Vertex>l1distance(
                        "distance",
                        "embedding",
                        vectorToSearchFor
                )
                .elementMap("name", "distance")
                .toList();
        for (Map<Object, Object> map : maps) {
            RecordId id = (RecordId) map.get(T.id);
            String name = (String) map.get("name");
            Double l2Distance = (Double) map.get("distance");
            Assert.assertEquals(l2Distance, recordIdDistanceMap.remove(id));
        }
        Assert.assertTrue(recordIdDistanceMap.isEmpty());

    }


    @Test
    public void testPgVectorQueryInnerProduct() {
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

        float[] vectorToSearchFor = new float[100];
        for (int i = 0; i < 100; i++) {
            vectorToSearchFor[i] = new Random().nextFloat();
        }

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                .<Vertex>innerProduct(
                        "distance",
                        "embedding",
                        vectorToSearchFor
                )
                .toList();
        Map<RecordId, Double> recordIdDistanceMap = new HashMap<>();
        for (Vertex vertex : vertices) {
            double l2distance = vertex.value("distance");
            recordIdDistanceMap.put((RecordId) vertex.id(), l2distance);
        }
        assertPgVector(sqlgGraph, vertices, """
                SELECT
                	"public"."V_PGVector"."ID" AS "alias1",
                	"public"."V_PGVector"."name" AS "alias2",
                	"public"."V_PGVector"."embedding" AS "alias3",
                	("embedding" <#> '[0.013207972,0.5191829,0.016266346,0.36312515,0.09715974,0.7004016,0.8037289,0.17637652,0.75307536,0.86576074,0.5792903,0.14926344,0.6025354,0.7725544,0.6670583,0.4929827,0.75677425,0.14727616,0.87836516,0.75937736,0.7959671,0.4385711,0.60409015,0.31040645,0.8505831,0.3097828,0.85263747,0.2711857,0.034220576,0.0034450293,0.8576316,0.16349697,0.067469716,0.23246193,0.96580803,0.67415786,0.040288746,0.24839157,0.7006174,0.040218353,0.74230176,0.82737064,0.5928458,0.8221752,0.37805742,0.05102992,0.9473907,0.62126136,0.6837731,0.689778,0.17082113,0.52562755,0.564943,0.034926772,0.073277414,0.25918287,0.16011697,0.84725004,0.21584737,0.7118595,0.8870184,0.24177808,0.059740663,0.6781068,0.9303504,0.4947033,0.350994,0.47838026,0.966976,0.55348635,0.6338031,0.44789523,0.9762635,0.36514664,0.854322,0.14553076,0.5975453,0.4605006,1.7422438E-4,0.07356751,0.2692225,0.7938732,0.56638366,0.33757365,0.20897722,0.20917702,0.6014197,0.85098714,0.37343687,0.10486883,0.3122658,0.36180395,0.1056025,0.68098193,0.50820607,0.14893168,0.6463439,0.03368807,0.72256225,0.3791278]') * -1 AS "distance"
                FROM
                	"public"."V_PGVector"
                """);

        List<Map<Object, Object>> maps = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                .<Vertex>innerProduct(
                        "innerProduct",
                        "embedding",
                        vectorToSearchFor
                )
                .elementMap("name", "innerProduct")
                .toList();
        for (Map<Object, Object> map : maps) {
            RecordId id = (RecordId) map.get(T.id);
            String name = (String) map.get("name");
            Double innerProduct = (Double) map.get("innerProduct");
            Assert.assertEquals(innerProduct, recordIdDistanceMap.remove(id));
        }
        Assert.assertTrue(recordIdDistanceMap.isEmpty());

    }

    @Test
    public void testPgVectorQueryCosineDistance() {
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

        float[] vectorToSearchFor = new float[100];
        for (int i = 0; i < 100; i++) {
            vectorToSearchFor[i] = new Random().nextFloat();
        }

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                .<Vertex>cosineDistance(
                        "distance",
                        "embedding",
                        vectorToSearchFor
                )
                .toList();
        Map<RecordId, Double> recordIdDistanceMap = new HashMap<>();
        for (Vertex vertex : vertices) {
            double l2distance = vertex.value("distance");
            recordIdDistanceMap.put((RecordId) vertex.id(), l2distance);
        }
        assertPgVector(sqlgGraph, vertices, """
                SELECT
                	"public"."V_PGVector"."ID" AS "alias1",
                	"public"."V_PGVector"."name" AS "alias2",
                	"public"."V_PGVector"."embedding" AS "alias3",
                	1 - ("embedding" <=> '[0.013207972,0.5191829,0.016266346,0.36312515,0.09715974,0.7004016,0.8037289,0.17637652,0.75307536,0.86576074,0.5792903,0.14926344,0.6025354,0.7725544,0.6670583,0.4929827,0.75677425,0.14727616,0.87836516,0.75937736,0.7959671,0.4385711,0.60409015,0.31040645,0.8505831,0.3097828,0.85263747,0.2711857,0.034220576,0.0034450293,0.8576316,0.16349697,0.067469716,0.23246193,0.96580803,0.67415786,0.040288746,0.24839157,0.7006174,0.040218353,0.74230176,0.82737064,0.5928458,0.8221752,0.37805742,0.05102992,0.9473907,0.62126136,0.6837731,0.689778,0.17082113,0.52562755,0.564943,0.034926772,0.073277414,0.25918287,0.16011697,0.84725004,0.21584737,0.7118595,0.8870184,0.24177808,0.059740663,0.6781068,0.9303504,0.4947033,0.350994,0.47838026,0.966976,0.55348635,0.6338031,0.44789523,0.9762635,0.36514664,0.854322,0.14553076,0.5975453,0.4605006,1.7422438E-4,0.07356751,0.2692225,0.7938732,0.56638366,0.33757365,0.20897722,0.20917702,0.6014197,0.85098714,0.37343687,0.10486883,0.3122658,0.36180395,0.1056025,0.68098193,0.50820607,0.14893168,0.6463439,0.03368807,0.72256225,0.3791278]') AS "distance"
                FROM
                	"public"."V_PGVector"
                """);

        List<Map<Object, Object>> maps = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                .<Vertex>cosineDistance(
                        "cosineDistance",
                        "embedding",
                        vectorToSearchFor
                )
                .elementMap("name", "cosineDistance")
                .toList();
        for (Map<Object, Object> map : maps) {
            RecordId id = (RecordId) map.get(T.id);
            String name = (String) map.get("name");
            Double cosineDistance = (Double) map.get("cosineDistance");
            Assert.assertEquals(cosineDistance, recordIdDistanceMap.remove(id));
        }
        Assert.assertTrue(recordIdDistanceMap.isEmpty());

    }

    @Test
    public void testPgVectorQueryHammingDistance() {
        int dimension = 3;
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                    put("embedding", PropertyDefinition.of(PropertyType.pgbit(dimension)));
                }});
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 100; i++) {
            boolean[] pgbits = new boolean[dimension];
            for (int j = 0; j < dimension; j++) {
                pgbits[j] = new Random().nextBoolean();
            }
            this.sqlgGraph.addVertex(T.label, "PGVector", "name", "a" + i, "embedding", new PGbit(pgbits));
        }
        this.sqlgGraph.tx().commit();

        boolean[] vectorToSearchFor = new boolean[dimension];
        for (int i = 0; i < dimension; i++) {
            vectorToSearchFor[i] = new Random().nextBoolean();
        }

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                .<Vertex>hammingDistance(
                        "distance",
                        "embedding",
                        vectorToSearchFor
                )
                .toList();
        Map<RecordId, Double> recordIdDistanceMap = new HashMap<>();
        for (Vertex vertex : vertices) {
            double l2distance = vertex.value("distance");
            recordIdDistanceMap.put((RecordId) vertex.id(), l2distance);
        }

        StringBuilder targetVectorAsString = new StringBuilder();
        for (boolean b : vectorToSearchFor) {
            targetVectorAsString.append(b ? "1" : "0");
        }

        assertPGbit(sqlgGraph, vertices, """
                SELECT
                	"public"."V_PGVector"."ID" AS "alias1",
                	"public"."V_PGVector"."name" AS "alias2",
                	"public"."V_PGVector"."embedding" AS "alias3",
                	("embedding" <~> '%s') AS "distance"
                FROM
                	"public"."V_PGVector"
                """.formatted(targetVectorAsString));

        List<Map<Object, Object>> maps = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                .<Vertex>hammingDistance(
                        "distance",
                        "embedding",
                        vectorToSearchFor
                )
                .elementMap("name", "distance")
                .toList();
        for (Map<Object, Object> map : maps) {
            RecordId id = (RecordId) map.get(T.id);
            String name = (String) map.get("name");
            Double l2Distance = (Double) map.get("distance");
            Assert.assertEquals(l2Distance, recordIdDistanceMap.remove(id));
        }
        Assert.assertTrue(recordIdDistanceMap.isEmpty());

    }

    @Test
    public void testPgVectorQueryJaccardDistance() {
        int dimension = 3;
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                    put("embedding", PropertyDefinition.of(PropertyType.pgbit(dimension)));
                }});
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 100; i++) {
            boolean[] pgbits = new boolean[dimension];
            for (int j = 0; j < dimension; j++) {
                pgbits[j] = new Random().nextBoolean();
            }
            this.sqlgGraph.addVertex(T.label, "PGVector", "name", "a" + i, "embedding", new PGbit(pgbits));
        }
        this.sqlgGraph.tx().commit();

        boolean[] vectorToSearchFor = new boolean[dimension];
        for (int i = 0; i < dimension; i++) {
            vectorToSearchFor[i] = new Random().nextBoolean();
        }

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                .<Vertex>jaccardDistance(
                        "distance",
                        "embedding",
                        vectorToSearchFor
                )
                .toList();
        Map<RecordId, Double> recordIdDistanceMap = new HashMap<>();
        for (Vertex vertex : vertices) {
            double l2distance = vertex.value("distance");
            recordIdDistanceMap.put((RecordId) vertex.id(), l2distance);
        }

        StringBuilder targetVectorAsString = new StringBuilder();
        for (boolean b : vectorToSearchFor) {
            targetVectorAsString.append(b ? "1" : "0");
        }

        assertPGbit(sqlgGraph, vertices, """
                SELECT
                	"public"."V_PGVector"."ID" AS "alias1",
                	"public"."V_PGVector"."name" AS "alias2",
                	"public"."V_PGVector"."embedding" AS "alias3",
                	("embedding" <%> '{targetVectorAsString}') AS "distance"
                FROM
                	"public"."V_PGVector"
                """.replace("{targetVectorAsString}", targetVectorAsString));

        List<Map<Object, Object>> maps = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                .<Vertex>jaccardDistance(
                        "distance",
                        "embedding",
                        vectorToSearchFor
                )
                .elementMap("name", "distance")
                .toList();
        for (Map<Object, Object> map : maps) {
            RecordId id = (RecordId) map.get(T.id);
            String name = (String) map.get("name");
            Double l2Distance = (Double) map.get("distance");
            Assert.assertEquals(l2Distance, recordIdDistanceMap.remove(id));
        }
        Assert.assertTrue(recordIdDistanceMap.isEmpty());

    }

    public static void assertPGbit(SqlgGraph sqlgGraph, List<Vertex> nearestNeighbours, String sql) {
        Connection connection = sqlgGraph.tx().getConnection();
        try (Statement s = connection.createStatement()) {
            ResultSet rs = s.executeQuery(sql);
            Map<Long, Pair<String, boolean[]>> expected = new HashMap<>();
            while (rs.next()) {
                long id = rs.getLong("alias1");
                String name = rs.getString("alias2");
                PGbit pgvector = rs.getObject("alias3", PGbit.class);
                boolean[] vector = pgvector.toArray();
                expected.put(id, Pair.of(name, vector));
            }
            for (Vertex nearestNeighbour : nearestNeighbours) {
                long id = ((RecordId) nearestNeighbour.id()).sequenceId();
                PGbit pBit= nearestNeighbour.value("embedding");
                boolean[] vector = pBit.toArray();
                String name = nearestNeighbour.value("name");

                Assert.assertTrue(expected.containsKey(id));

                Pair<String, boolean[]> pair = expected.remove(id);
                Assert.assertEquals(name, pair.getKey());
                Assert.assertArrayEquals(vector, pair.getValue());
            }
            Assert.assertTrue(expected.isEmpty());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertPgVector(SqlgGraph sqlgGraph, List<Vertex> nearestNeighbours, String sql) {
        Connection connection = sqlgGraph.tx().getConnection();
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
