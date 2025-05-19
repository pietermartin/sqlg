package org.umlg.sqlg.predicate;

import org.umlg.sqlg.predicate.PGVectorPredicate.QueryType;

import java.util.Comparator;

public class PGVectorOrderByComparator implements Comparator<float[]> {

    private final QueryType queryType;
    private final String operator;
    private final boolean[] bitVector;
    private final float[] vector;

    public static PGVectorOrderByComparator l2distance(float[] vector) {
        return new PGVectorOrderByComparator(QueryType.L2_DISTANCE, vector, null, "<->");
    }

    public static PGVectorOrderByComparator l1distance(float[] vector) {
        return new PGVectorOrderByComparator(QueryType.L1_DISTACNE, vector, null, "<+>");
    }
    
    public static PGVectorOrderByComparator innerProduct(float[] vector) {
        return new PGVectorOrderByComparator(QueryType.INNER_PRODUCT, vector, null, "<#>");
    }

    public static PGVectorOrderByComparator cosineDistance(float[] vector) {
        return new PGVectorOrderByComparator(QueryType.COSINE_DISTANCE, vector, null, "<=>");
    }
    
    public static PGVectorOrderByComparator hammingDistance(boolean[] vector) {
        return new PGVectorOrderByComparator(QueryType.HAMMING_DISTANCE, null, vector, "<~>");
    }
    
    public static PGVectorOrderByComparator jaccardDistance(boolean[] vector) {
        return new PGVectorOrderByComparator(QueryType.JACCARD_DISTANCE, null, vector, "<%>");
    }

    private PGVectorOrderByComparator(QueryType queryTypes, float[] vector, boolean[] bitVector, String operator) {
        this.queryType = queryTypes;
        this.vector = vector;
        this.bitVector = bitVector;
        this.operator = operator;
    }

    @Override
    public int compare(float[] o1, float[] o2) {
        throw new IllegalStateException("Should not be called");
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public float[] getVector() {
        return vector;
    }

    public boolean[] getBitVector() {
        return bitVector;
    }

    public String getOperator() {
        return operator;
    }
}
