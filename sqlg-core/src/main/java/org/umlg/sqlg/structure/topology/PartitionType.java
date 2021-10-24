package org.umlg.sqlg.structure.topology;

public enum PartitionType {
    NONE,
    RANGE,
    LIST,
    HASH;

    public boolean isNone() {
        return this == NONE;
    }

    public boolean isRange() {
        return this == RANGE;
    }

    public boolean isList() {
        return this == LIST;
    }

    public boolean isHash() {
        return this == HASH;
    }

    public static PartitionType fromPostgresPartStrat(String partitionType) {
        switch (partitionType) {
            case "r":
                return PartitionType.RANGE;
            case "l":
                return PartitionType.LIST;
            case "h":
                return PartitionType.HASH;
            default:
                throw new IllegalArgumentException(String.format("postgres partition type flag %s not supported", partitionType));

        }
    }

    public static PartitionType from(String partitionType) {
        switch (partitionType) {
            case "NONE":
                return NONE;
            case "RANGE":
                return RANGE;
            case "LIST":
                return LIST;
            case "HASH":
                return HASH;
            default:
                throw new IllegalArgumentException(String.format("Unknown PartitionType %s", partitionType));
        }
    }
}
