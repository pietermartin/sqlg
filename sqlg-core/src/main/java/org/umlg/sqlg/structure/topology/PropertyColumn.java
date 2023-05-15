package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.TopologyInf;

/**
 * Date: 2016/09/04
 * Time: 8:50 AM
 */
public class PropertyColumn implements TopologyInf {

    private final AbstractLabel abstractLabel;
    private final String name;
    private boolean committed = true;
    private final PropertyDefinition propertyDefinition;
    @SuppressWarnings("unused")
    private final boolean isForeignPropertyColumn;

    PropertyColumn(AbstractLabel abstractLabel, String name, PropertyDefinition propertyDefinition) {
        this.abstractLabel = abstractLabel;
        this.name = name;
        this.propertyDefinition = propertyDefinition;
        this.isForeignPropertyColumn = false;
    }

    private PropertyColumn(AbstractLabel abstractLabel, String name, PropertyDefinition propertyDefinition, boolean isForeignPropertyColumn) {
        Preconditions.checkState(isForeignPropertyColumn);
        this.abstractLabel = abstractLabel;
        this.name = name;
        this.propertyDefinition = propertyDefinition;
        this.isForeignPropertyColumn = true;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean isCommitted() {
        return this.committed;
    }

    public PropertyType getPropertyType() {
        return propertyDefinition.propertyType();
    }

    public PropertyDefinition getPropertyDefinition() {
        return propertyDefinition;
    }

    public AbstractLabel getParentLabel() {
        return abstractLabel;
    }

    void setCommitted(boolean committed) {
        this.committed = committed;
    }

    void afterCommit() {
        Preconditions.checkState(this.getParentLabel().getTopology().isSchemaChanged(), "PropertyColumn.afterCommit must have schemaChanged = true");
        this.committed = true;
    }

    void afterRollback() {
        Preconditions.checkState(this.getParentLabel().getTopology().isSchemaChanged(), "PropertyColumn.afterRollback must have schemaChanged = true");
    }

    ObjectNode toNotifyJson() {
        ObjectNode propertyObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        propertyObjectNode.put("name", this.name);
        propertyObjectNode.set("propertyDefinition", this.propertyDefinition.toNotifyJson());
        return propertyObjectNode;
    }

    static PropertyColumn fromNotifyJson(AbstractLabel abstractLabel, JsonNode jsonNode) {
        return new PropertyColumn(
                abstractLabel,
                jsonNode.get("name").asText(),
                PropertyDefinition.fromNotifyJson(jsonNode.get("propertyDefinition")));
    }

    @Override
    public int hashCode() {
        return (this.abstractLabel.getSchema().getName() + this.abstractLabel.getLabel() + this.getName()).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof PropertyColumn other)) {
            return false;
        }
        return this.abstractLabel.getSchema().getName().equals(other.abstractLabel.getSchema().getName()) &&
                this.abstractLabel.getLabel().equals(other.abstractLabel.getLabel()) &&
                this.getName().equals(other.getName()) &&
                this.getPropertyDefinition().equals(other.getPropertyDefinition());
    }

    @Override
    public String toString() {
        return this.abstractLabel.getSchema().getName() + "." + this.abstractLabel.getLabel() + "." + this.name;
    }


    @Override
    public void remove(boolean preserveData) {
        this.abstractLabel.removeProperty(this, preserveData);
    }

    @Override
    public void rename(String name) {
        this.abstractLabel.renameProperty(name, this);
    }

    @Override
    public void updatePropertyDefinition(PropertyDefinition propertyDefinition) {
        this.abstractLabel.updatePropertyDefinition(this, propertyDefinition);
    }

    PropertyColumn readOnlyCopy(AbstractLabel abstractLabel) {
        return new PropertyColumn(abstractLabel, this.name, this.propertyDefinition, true);
    }
}
