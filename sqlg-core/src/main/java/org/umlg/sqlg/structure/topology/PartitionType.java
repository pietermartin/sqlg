package org.umlg.sqlg.structure.topology;

public enum PartitionType {
    NONE,
    RANGE,
    LIST;

    public boolean isNone() {
        return this == NONE;
    }

    public boolean isRange() {
        return this == RANGE;
    }

    public boolean isList() {
        return this == LIST;
    }
}
