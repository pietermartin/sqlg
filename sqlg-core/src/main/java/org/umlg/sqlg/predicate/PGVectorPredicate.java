package org.umlg.sqlg.predicate;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;

public record PGVectorPredicate(String operator, float[] vector, double distance) implements PBiPredicate<Double, Double> {

    public static P<Double> l2Distance(final float[] vector, final double distance) {
        return new P<>(new PGVectorPredicate("<->", vector, distance), distance);
    }
    
    public static P<Double> negativeInnerProduct(final float[] vector, final double distance) {
        return new P<>(new PGVectorPredicate("<#>", vector, distance), distance);
    }

    @Override
    public boolean test(Double s, Double s2) {
        return false;
    }
    
}
