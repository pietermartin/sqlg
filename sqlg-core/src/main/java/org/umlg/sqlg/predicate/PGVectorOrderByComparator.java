package org.umlg.sqlg.predicate;

import java.util.Comparator;

public class PGVectorOrderByComparator implements Comparator<float[]> {

    private final float[] vector;

    public PGVectorOrderByComparator(float[] vector) {
        this.vector = vector;
    }

    @Override
    public int compare(float[] o1, float[] o2) {
        return 0;
    }

    public float[] getVector() {
        return vector;
    }
}
