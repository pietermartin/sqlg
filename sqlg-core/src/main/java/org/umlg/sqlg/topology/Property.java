package org.umlg.sqlg.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.umlg.sqlg.structure.PropertyType;

/**
 * Date: 2016/09/04
 * Time: 8:50 AM
 */
public class Property {

    private String name;
    private PropertyType propertyType;

    public Property(String name, PropertyType propertyType) {
        this.name = name;
        this.propertyType = propertyType;
    }

    public String getName() {
        return name;
    }

    public PropertyType getPropertyType() {
        return propertyType;
    }

    public void afterCommit() {

    }

    public void afterRollback() {

    }

    public JsonNode toNotifyJson() {
        ObjectNode propertyObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        propertyObjectNode.put("name", this.name);
        propertyObjectNode.put("propertyType", this.propertyType.name());
        return propertyObjectNode;
    }

    public static Property fromNotifyJson(JsonNode jsonNode) {
        Property property = new Property(
                jsonNode.get("name").asText(),
                PropertyType.from(jsonNode.get("propertyType").asText()));
        return property;
    }
}
