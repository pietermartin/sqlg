package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.umlg.sqlg.structure.topology.Topology;

/**
 * Represents a Vertex or Edge property.
 * @param propertyType The type of the property
 * @param multiplicity The multiplicity of the property
 * @param defaultLiteral The default value of the property
 * @param checkConstraint A constraint on valid values for the property.
 * @param temp Only to be used internally. If true then the multiplicity will not be validated.
 */
public record PropertyDefinition(PropertyType propertyType, Multiplicity multiplicity, String defaultLiteral,
                                 String checkConstraint, boolean temp) {
    public PropertyDefinition {
        if (!propertyType.isArray() && multiplicity.isMany()) {
            throw new IllegalArgumentException("Multiplicity can only be a many for array types.");
        }
        if (!propertyType.isArray() && multiplicity.upper() != 1) {
            throw new IllegalArgumentException("upper Multiplicity must be 1 for non array types.");
        }
    }

    private PropertyDefinition(PropertyType propertyType) {
        this(propertyType, (propertyType.isArray() ? Multiplicity.of(0, -1) : Multiplicity.of()), null, null, false);
    }

    private PropertyDefinition(PropertyType propertyType, Multiplicity multiplicity) {
        this(propertyType, multiplicity, null, null, false);
    }

    public static PropertyDefinition of(PropertyType propertyType) {
        return new PropertyDefinition(propertyType);
    }

    public static PropertyDefinition temp(PropertyType propertyType) {
        return new PropertyDefinition(propertyType, (propertyType.isArray() ? Multiplicity.of(0, -1) : Multiplicity.of()), null, null, true);
    }

    public static PropertyDefinition of(PropertyType propertyType, Multiplicity multiplicity) {
        return new PropertyDefinition(propertyType, multiplicity);
    }

    public static PropertyDefinition of(PropertyType propertyType, Multiplicity multiplicity, String defaultLiteral) {
        return new PropertyDefinition(propertyType, multiplicity, defaultLiteral, null, false);
    }

    public static PropertyDefinition of(PropertyType propertyType, Multiplicity multiplicity, String defaultLiteral, String checkConstraint) {
        return new PropertyDefinition(propertyType, multiplicity, defaultLiteral, checkConstraint, false);
    }

    public ObjectNode toNotifyJson() {
        ObjectNode propertyDefinitionObjectNode = Topology.OBJECT_MAPPER.createObjectNode();
        propertyDefinitionObjectNode.put("propertyType", propertyType.name());
        propertyDefinitionObjectNode.set("multiplicity", multiplicity.toNotifyJson());
        propertyDefinitionObjectNode.put("defaultLiteral", defaultLiteral);
        propertyDefinitionObjectNode.put("checkConstraint", checkConstraint);
        propertyDefinitionObjectNode.put("temp", temp);
        return propertyDefinitionObjectNode;
    }

    public static PropertyDefinition fromNotifyJson(JsonNode jsonNode) {
        String pt = jsonNode.get("propertyType").asText();
        PropertyType propertyType;
        if (pt.equals("VARCHAR")) {
            //This is not ideal, however Sqlg only uses VARCHAR when creating the column.
            //For the rest it is considered the same as STRING
            propertyType = PropertyType.STRING;
        } else {
            propertyType = PropertyType.valueOf(pt);
        }
        String defaultLiteral = !jsonNode.get("defaultLiteral").isNull() ? jsonNode.get("defaultLiteral").asText() : null;
        String checkConstraint = !jsonNode.get("checkConstraint").isNull() ? jsonNode.get("checkConstraint").asText() : null;
        return PropertyDefinition.of(propertyType, Multiplicity.fromNotifyJson(jsonNode.get("multiplicity")), defaultLiteral, checkConstraint);
    }
}
