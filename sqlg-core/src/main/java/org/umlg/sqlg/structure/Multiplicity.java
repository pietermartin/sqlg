package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.umlg.sqlg.structure.topology.Topology;

public record Multiplicity(long lower, long upper) {

    public Multiplicity {
        if (lower < -1) {
            throw new IllegalArgumentException("Multiplicity.lower must be >= -1");
        }
        if (upper < -1) {
            throw new IllegalArgumentException("Multiplicity.upper must be >= -1");
        }
    }

    public static Multiplicity from(long lower, long higher) {
        return new Multiplicity(lower, higher);
    }

    public Multiplicity() {
        this(0, 1);
    }

    public boolean isMany() {
        return upper > 1 || upper == -1;
    }

    public boolean isRequired() {
        return lower > 0;
    }

    public ObjectNode toNotifyJson() {
        ObjectNode multiplicityObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        multiplicityObjectNode.put("lower", lower);
        multiplicityObjectNode.put("upper", upper);
        return multiplicityObjectNode;
    }

    public static Multiplicity fromNotifyJson(JsonNode jsonNode) {
        return Multiplicity.from(jsonNode.get("lower").asLong(), jsonNode.get("upper").asLong());
    }
}
