package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.umlg.sqlg.structure.topology.Topology;

public record Multiplicity(long lower, long upper, boolean unique, boolean ordered) {

    public Multiplicity {
        if (lower < -1) {
            throw new IllegalArgumentException("Multiplicity.lower must be >= -1");
        }
        if (upper < -1) {
            throw new IllegalArgumentException("Multiplicity.upper must be >= -1");
        }
        if (upper != -1 && (lower > upper)) {
            throw new IllegalArgumentException(String.format("Multiplicity.lower[%d] must be <= Multiplicity.upper[%d]", lower, upper));
        }
    }

    public static Multiplicity of() {
        return new Multiplicity(0, 1, false, false);
    }

    public static Multiplicity of(long lower, long higher) {
        return new Multiplicity(lower, higher, false, false);
    }

    public static Multiplicity of(long lower, long higher, boolean unique) {
        return new Multiplicity(lower, higher, unique, false);
    }

    private static Multiplicity of(long lower, long higher, boolean unique, boolean ordered) {
        return new Multiplicity(lower, higher, unique, ordered);
    }

    public boolean isMany() {
        return upper > 1 || upper == -1;
    }

    public boolean isOne() {
        return !isMany();
    }

    public boolean isRequired() {
        return lower > 0;
    }

    public boolean hasLimits() {
        return lower > 0 || upper != -1;
    }

    public String toCheckConstraint(String column) {
        Preconditions.checkState(hasLimits());
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        if (lower > 0) {
            sb.append("(CARDINALITY(").append(column).append(") >= ").append(lower).append(")");
        }
        if (upper != -1) {
            if (lower > 0) {
                sb.append(" AND ");
            }
            sb.append("(CARDINALITY(").append(column).append(") <= ").append(upper).append(")");
        }
        sb.append(")");
        return sb.toString();
    }

    public ObjectNode toNotifyJson() {
        ObjectNode multiplicityObjectNode = Topology.OBJECT_MAPPER.createObjectNode();
        multiplicityObjectNode.put("lower", lower);
        multiplicityObjectNode.put("upper", upper);
        multiplicityObjectNode.put("unique", unique);
        multiplicityObjectNode.put("ordered", ordered);
        return multiplicityObjectNode;
    }

    public static Multiplicity fromNotifyJson(JsonNode jsonNode) {
        return Multiplicity.of(
                jsonNode.get("lower").asLong(),
                jsonNode.get("upper").asLong(),
                jsonNode.get("unique").asBoolean(),
                jsonNode.get("ordered").asBoolean());
    }


    @Override
    public String toString() {
        return "[" + lower + ", " + upper + "][unique:" + unique + "][ordered:" + ordered + "]";
    }
}
