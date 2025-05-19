package org.umlg.sqlg.predicate;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;

public record PGVectorPredicate(QueryType queryType, String operator, float[] vector,
                                boolean[] bitVector, double distance) implements PBiPredicate<Double, Double> {

    public enum QueryType {
        L2_DISTANCE,
        L1_DISTACNE,
        INNER_PRODUCT,
        COSINE_DISTANCE,
        HAMMING_DISTANCE,
        JACCARD_DISTANCE
    }

    public static P<Double> l2Distance(final float[] vector, final double distance) {
        return new P<>(new PGVectorPredicate(QueryType.L2_DISTANCE, "<->", vector, null, distance), distance);
    }

    public static P<Double> l1Distance(final float[] vector, final double distance) {
        return new P<>(new PGVectorPredicate(QueryType.L1_DISTACNE, "<+>", vector, null, distance), distance);
    }

    public static P<Double> innerProduct(final float[] vector, final double distance) {
        return new P<>(new PGVectorPredicate(QueryType.INNER_PRODUCT, "<#>", vector, null, distance), distance);
    }

    public static P<Double> cosineDistance(final float[] vector, final double distance) {
        return new P<>(new PGVectorPredicate(QueryType.COSINE_DISTANCE, "<=>", vector, null, distance), distance);
    }

    public static P<Double> hammingDistance(final boolean[] vector, final double distance) {
        return new P<>(new PGVectorPredicate(QueryType.HAMMING_DISTANCE, "<~>", null, vector, distance), distance);
    }

    public static P<Double> jaccardDistance(final boolean[] vector, final double distance) {
        return new P<>(new PGVectorPredicate(QueryType.JACCARD_DISTANCE, "<%>", null, vector, distance), distance);
    }

    @Override
    public boolean test(Double s, Double s2) {
        return false;
    }

}
