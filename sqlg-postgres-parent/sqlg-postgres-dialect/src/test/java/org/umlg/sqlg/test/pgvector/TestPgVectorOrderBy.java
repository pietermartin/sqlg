package org.umlg.sqlg.test.pgvector;

import com.pgvector.PGbit;
import com.pgvector.PGvector;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.predicate.PGVectorOrderByComparator;
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

public class TestPgVectorOrderBy extends BaseTest {

    @Test
    public void testPgVectorQueryOrderBy() {
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

            //l2distance
            List<Vertex> nearestNeighbours = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                    .order().by("embedding", PGVectorOrderByComparator.l2distance(toSearchFor))
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
            TestPgVectorQueryDistance.assertPgVector(sqlgGraph, nearestNeighbours, sql);

            //l1distance
            nearestNeighbours = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                    .order().by("embedding", PGVectorOrderByComparator.l1distance(toSearchFor))
                    .limit(5)
                    .toList();
            sql = """
                    SELECT
                    	"public"."V_PGVector"."ID" AS "alias1",
                    	"public"."V_PGVector"."name" AS "alias2",
                    	"public"."V_PGVector"."embedding" AS "alias3"
                    FROM
                    	"public"."V_PGVector"
                    ORDER BY
                    	 "public"."V_PGVector"."embedding" <+> %s
                    LIMIT 5 OFFSET 0
                    """.formatted(floatToSearchForAsLiteral);
            TestPgVectorQueryDistance.assertPgVector(sqlgGraph, nearestNeighbours, sql);

            //innerProduct
            nearestNeighbours = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                    .order().by("embedding", PGVectorOrderByComparator.innerProduct(toSearchFor))
                    .limit(5)
                    .toList();
            sql = """
                    SELECT
                    	"public"."V_PGVector"."ID" AS "alias1",
                    	"public"."V_PGVector"."name" AS "alias2",
                    	"public"."V_PGVector"."embedding" AS "alias3"
                    FROM
                    	"public"."V_PGVector"
                    ORDER BY
                    	 ("public"."V_PGVector"."embedding" <#> %s) * -1
                    LIMIT 5 OFFSET 0
                    """.formatted(floatToSearchForAsLiteral);
            TestPgVectorQueryDistance.assertPgVector(sqlgGraph, nearestNeighbours, sql);

            //cosineDistance
            nearestNeighbours = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                    .order().by("embedding", PGVectorOrderByComparator.cosineDistance(toSearchFor))
                    .limit(5)
                    .toList();
            sql = """
                    SELECT
                    	"public"."V_PGVector"."ID" AS "alias1",
                    	"public"."V_PGVector"."name" AS "alias2",
                    	"public"."V_PGVector"."embedding" AS "alias3"
                    FROM
                    	"public"."V_PGVector"
                    ORDER BY
                    	 1 - ("public"."V_PGVector"."embedding" <=> %s)
                    LIMIT 5 OFFSET 0
                    """.formatted(floatToSearchForAsLiteral);
            TestPgVectorQueryDistance.assertPgVector(sqlgGraph, nearestNeighbours, sql);
        }
    }

    @Test
    public void testPgBitQueryOrderBy() {
        int dimension = 3;
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist("PGVector", new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                    put("embedding", PropertyDefinition.of(PropertyType.pgbit(dimension)));
                }});
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 100; i++) {
            boolean[] booleans = new boolean[dimension];
            for (int j = 0; j < dimension; j++) {
                booleans[j] = new Random().nextBoolean();
            }
            this.sqlgGraph.addVertex(T.label, "PGVector", "name", "a", "embedding", new PGbit(booleans));
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
        Assert.assertEquals(100, pgvectors.size());

        for (int k = 0; k < 100; k++) {
            boolean[] toSearchFor = new boolean[dimension];
            StringBuilder floatToSearchForAsLiteral = new StringBuilder("'");
            for (int j = 0; j < dimension; j++) {
                toSearchFor[j] = new Random().nextBoolean();
                floatToSearchForAsLiteral.append(toSearchFor[j] ? "1" : "0");
            }
            floatToSearchForAsLiteral.append("'");

            //hammingDistance
            List<Vertex> nearestNeighbours = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                    .order().by("embedding", PGVectorOrderByComparator.hammingDistance(toSearchFor))
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
                    	 "public"."V_PGVector"."embedding" <~> {floatToSearchForAsLiteral}
                    LIMIT 5 OFFSET 0
                    """.replace("{floatToSearchForAsLiteral}", floatToSearchForAsLiteral.toString());
            assertPGbit(nearestNeighbours, sql);

            //jaccardDistance
            nearestNeighbours = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                    .order().by("embedding", PGVectorOrderByComparator.jaccardDistance(toSearchFor))
                    .limit(5)
                    .toList();
            sql = """
                    SELECT
                    	"public"."V_PGVector"."ID" AS "alias1",
                    	"public"."V_PGVector"."name" AS "alias2",
                    	"public"."V_PGVector"."embedding" AS "alias3"
                    FROM
                    	"public"."V_PGVector"
                    ORDER BY
                    	 "public"."V_PGVector"."embedding" <%> {floatToSearchForAsLiteral}
                    LIMIT 5 OFFSET 0
                    """.replace("{floatToSearchForAsLiteral}", floatToSearchForAsLiteral);
            assertPGbit(nearestNeighbours, sql);
        }
    }

    public void assertPGbit(List<Vertex> nearestNeighbours, String sql) {
        Connection connection = this.sqlgGraph.tx().getConnection();
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

}
