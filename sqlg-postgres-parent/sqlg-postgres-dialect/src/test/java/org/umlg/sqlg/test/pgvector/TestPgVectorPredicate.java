package org.umlg.sqlg.test.pgvector;

import com.pgvector.PGbit;
import com.pgvector.PGvector;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.predicate.PGVectorPredicate;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class TestPgVectorPredicate extends BaseTest {

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
            TestPgVectorQueryDistance.assertPgVector(sqlgGraph, nearestNeighbours, sql);
        }
    }

    @Test
    public void testPgVectorQueryL1DistancePredicate() {
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
                    .has("embedding", PGVectorPredicate.l1Distance(toSearchFor, 5))
                    .toList();
            String sql = """
                    SELECT
                    	"public"."V_PGVector"."ID" AS "alias1",
                    	"public"."V_PGVector"."name" AS "alias2",
                    	"public"."V_PGVector"."embedding" AS "alias3"
                    FROM
                    	"public"."V_PGVector"
                    WHERE "public"."V_PGVector"."embedding" <+> %s < %d
                    """.formatted(floatToSearchForAsLiteral, 5);
            TestPgVectorQueryDistance.assertPgVector(sqlgGraph, nearestNeighbours, sql);
        }
    }

    @Test
    public void testPgVectorQueryInnerProductPredicate() {
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
                    .has("embedding", PGVectorPredicate.innerProduct(toSearchFor, 5))
                    .toList();
            String sql = """
                    SELECT
                    	"public"."V_PGVector"."ID" AS "alias1",
                    	"public"."V_PGVector"."name" AS "alias2",
                    	"public"."V_PGVector"."embedding" AS "alias3"
                    FROM
                    	"public"."V_PGVector"
                    WHERE ("public"."V_PGVector"."embedding" <#> %s) * -1 < %d
                    """.formatted(floatToSearchForAsLiteral, 5);
            TestPgVectorQueryDistance.assertPgVector(sqlgGraph, nearestNeighbours, sql);
        }
    }

    @Test
    public void testPgVectorQueryCosineDistancePredicate() {
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
                    .has("embedding", PGVectorPredicate.cosineDistance(toSearchFor, 5))
                    .toList();
            String sql = """
                    SELECT
                    	"public"."V_PGVector"."ID" AS "alias1",
                    	"public"."V_PGVector"."name" AS "alias2",
                    	"public"."V_PGVector"."embedding" AS "alias3"
                    FROM
                    	"public"."V_PGVector"
                    WHERE (1 - ("public"."V_PGVector"."embedding" <=> %s)) < %d
                    """.formatted(floatToSearchForAsLiteral, 5);
            TestPgVectorQueryDistance.assertPgVector(sqlgGraph, nearestNeighbours, sql);
        }
    }

    @Test
    public void testPgVectorQueryHammingDistancePredicate() {
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
        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
        Assert.assertEquals(100, pgvectors.size());


        for (int k = 0; k < 100; k++) {
            boolean[] vectorToSearchFor = new boolean[dimension];
            for (int i = 0; i < dimension; i++) {
                vectorToSearchFor[i] = new Random().nextBoolean();
            }

            List<Vertex> nearestNeighbours = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                    .has("embedding", PGVectorPredicate.hammingDistance(vectorToSearchFor, 5))
                    .toList();
            StringBuilder targetVectorAsString = new StringBuilder();
            for (boolean b : vectorToSearchFor) {
                targetVectorAsString.append(b ? "1" : "0");
            }
            String sql = """
                    SELECT
                    	"public"."V_PGVector"."ID" AS "alias1",
                    	"public"."V_PGVector"."name" AS "alias2",
                    	"public"."V_PGVector"."embedding" AS "alias3"
                    FROM
                    	"public"."V_PGVector"
                    WHERE ("public"."V_PGVector"."embedding" <~> '%s') < %d
                    """.formatted(targetVectorAsString, 5);
            TestPgVectorQueryDistance.assertPGbit(sqlgGraph, nearestNeighbours, sql);
        }
    }

    @Test
    public void testPgVectorQueryJaccardDistancePredicate() {
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
        List<Vertex> pgvectors = this.sqlgGraph.traversal().V().hasLabel("PGVector").toList();
        Assert.assertEquals(100, pgvectors.size());


        for (int k = 0; k < 100; k++) {
            boolean[] vectorToSearchFor = new boolean[dimension];
            for (int i = 0; i < dimension; i++) {
                vectorToSearchFor[i] = new Random().nextBoolean();
            }

            List<Vertex> nearestNeighbours = this.sqlgGraph.traversal().V().hasLabel("PGVector")
                    .has("embedding", PGVectorPredicate.jaccardDistance(vectorToSearchFor, 5))
                    .toList();
            StringBuilder targetVectorAsString = new StringBuilder();
            for (boolean b : vectorToSearchFor) {
                targetVectorAsString.append(b ? "1" : "0");
            }
            String sql = """
                    SELECT
                    	"public"."V_PGVector"."ID" AS "alias1",
                    	"public"."V_PGVector"."name" AS "alias2",
                    	"public"."V_PGVector"."embedding" AS "alias3"
                    FROM
                    	"public"."V_PGVector"
                    WHERE ("public"."V_PGVector"."embedding" <%> '{targetVectorAsString}') < {distance}
                    """.replace("{targetVectorAsString}", targetVectorAsString).replace("{distance}", "5");
            TestPgVectorQueryDistance.assertPGbit(sqlgGraph, nearestNeighbours, sql);
        }
    }
}
