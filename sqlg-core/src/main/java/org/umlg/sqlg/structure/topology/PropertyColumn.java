package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.TopologyInf;

/**
 * Date: 2016/09/04
 * Time: 8:50 AM
 */
public class PropertyColumn implements TopologyInf {

    private final AbstractLabel abstractLabel;
    private String name;
    private boolean committed = true;
    private final PropertyType propertyType;
    private final boolean isForeignPropertyColumn;;

    PropertyColumn(AbstractLabel abstractLabel, String name, PropertyType propertyType) {
        this.abstractLabel = abstractLabel;
        this.name = name;
        this.propertyType = propertyType;
        this.isForeignPropertyColumn = false;
    }

    private PropertyColumn(AbstractLabel abstractLabel, String name, PropertyType propertyType, boolean isForeignPropertyColumn) {
        Preconditions.checkState(isForeignPropertyColumn);
        this.abstractLabel = abstractLabel;
        this.name = name;
        this.propertyType = propertyType;
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
        return propertyType;
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
        propertyObjectNode.put("propertyType", this.propertyType.name());
        return propertyObjectNode;
    }

    static PropertyColumn fromNotifyJson(AbstractLabel abstractLabel, JsonNode jsonNode) {
       String pt = jsonNode.get("propertyType").asText();
       if (pt.equals("VARCHAR")) {
           //This is not ideal, however Sqlg only uses VARCHAR when creating the column.
           //For the rest is is considered the same as STRING
           return new PropertyColumn(
                   abstractLabel,
                   jsonNode.get("name").asText(),
                   PropertyType.STRING);
       } else {
           return new PropertyColumn(
                   abstractLabel,
                   jsonNode.get("name").asText(),
                   PropertyType.valueOf(pt));
       }
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
        if (!(o instanceof PropertyColumn)) {
            return false;
        }
        PropertyColumn other = (PropertyColumn) o;
        if (this.abstractLabel.getSchema().getName().equals(other.abstractLabel.getSchema().getName())) {
            if (this.abstractLabel.getLabel().equals(other.abstractLabel.getLabel())) {
                return this.getName().equals(other.getName()) && this.getPropertyType() == other.getPropertyType();
            } else {
                return false;
            }
        } else {
            return false;
        }
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

    /**
     * Only called from afterRollback to reset the property's name.
     * @param name The old name  of the property.
     */
    void setName(String name) {
        this.name = name;
    }

    PropertyColumn readOnlyCopy(AbstractLabel abstractLabel) {
        return new PropertyColumn(abstractLabel, this.name, this.propertyType, true);
    }
}
