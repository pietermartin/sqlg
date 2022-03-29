package org.umlg.sqlg.structure;

public record Multiplicity(int lower, int upper) {

    public Multiplicity {
        if (lower < -1) {
            throw new IllegalArgumentException("Multiplicity.lower must be >= -1");
        }
        if (upper < -1) {
            throw new IllegalArgumentException("Multiplicity.upper must be >= -1");
        }
    }

    public Multiplicity() {
        this(0, 1);
    }

    public boolean isMany() {
        return upper > 1 || upper == -1;
    }
}
