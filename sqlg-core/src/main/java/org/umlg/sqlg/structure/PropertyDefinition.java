package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.umlg.sqlg.structure.topology.Topology;

public record PropertyDefinition(PropertyType propertyType, Multiplicity multiplicity, String defaultLiteral,
                                 String checkConstraint) {
    public PropertyDefinition {
        if (!propertyType.isArray() && multiplicity.isMany()) {
            throw new IllegalArgumentException("Multiplicity can only be a many for array types.");
        }
    }

    public PropertyDefinition(PropertyType propertyType) {
        this(propertyType, (propertyType.isArray() ? Multiplicity.of(0, -1) : Multiplicity.of()), null, null);
    }

    public PropertyDefinition(PropertyType propertyType, Multiplicity multiplicity) {
        this(propertyType, multiplicity, null, null);
    }

    public static PropertyDefinition of(PropertyType propertyType) {
        return new PropertyDefinition(propertyType);
    }

    public static PropertyDefinition of(PropertyType propertyType, Multiplicity multiplicity) {
        return new PropertyDefinition(propertyType, multiplicity);
    }

    public static PropertyDefinition of(PropertyType propertyType, Multiplicity multiplicity, String defaultLiteral) {
        return new PropertyDefinition(propertyType, multiplicity, defaultLiteral, null);
    }

    public static PropertyDefinition of(PropertyType propertyType, Multiplicity multiplicity, String defaultLiteral, String checkConstraint) {
        return new PropertyDefinition(propertyType, multiplicity, defaultLiteral, checkConstraint);
    }

    public ObjectNode toNotifyJson() {
        ObjectNode propertyDefinitionObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        propertyDefinitionObjectNode.put("propertyType", propertyType.name());
        propertyDefinitionObjectNode.set("multiplicity", multiplicity.toNotifyJson());
        propertyDefinitionObjectNode.put("defaultLiteral", defaultLiteral);
        propertyDefinitionObjectNode.put("checkConstraint", checkConstraint);
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
