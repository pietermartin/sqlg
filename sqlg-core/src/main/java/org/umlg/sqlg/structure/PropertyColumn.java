package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Date: 2016/09/04
 * Time: 8:50 AM
 */
public class PropertyColumn implements TopologyInf {

    private AbstractLabel abstractLabel;
    private String name;
    private PropertyType propertyType;
    private Set<GlobalUniqueIndex> globalUniqueIndices = new HashSet<>();
    private Set<GlobalUniqueIndex> uncommittedGlobalUniqueIndices = new HashSet<>();

    PropertyColumn(AbstractLabel abstractLabel, String name, PropertyType propertyType) {
        this.abstractLabel = abstractLabel;
        this.name = name;
        this.propertyType = propertyType;
    }

    public String getName() {
        return name;
    }

    public PropertyType getPropertyType() {
        return propertyType;
    }

    AbstractLabel getAbstractLabel() {
        return abstractLabel;
    }

    public Set<GlobalUniqueIndex> getGlobalUniqueIndices() {
        HashSet<GlobalUniqueIndex> result = new HashSet<>();
        result.addAll(this.globalUniqueIndices);
        if (this.abstractLabel.getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            result.addAll(this.uncommittedGlobalUniqueIndices);
        }
        return result;
    }

    void afterCommit() {
        if (this.abstractLabel.getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            Iterator<GlobalUniqueIndex> globalUniqueIndexIter = this.uncommittedGlobalUniqueIndices.iterator();
            while (globalUniqueIndexIter.hasNext()) {
                GlobalUniqueIndex globalUniqueIndex = globalUniqueIndexIter.next();
                this.globalUniqueIndices.add(globalUniqueIndex);
                globalUniqueIndexIter.remove();
            }
        }
    }

    void afterRollback() {
        if (this.abstractLabel.getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            this.uncommittedGlobalUniqueIndices.clear();
        }
    }

    void addGlobalUniqueIndex(GlobalUniqueIndex globalUniqueIndex) {
        this.uncommittedGlobalUniqueIndices.add(globalUniqueIndex);
        this.abstractLabel.addGlobalUniqueIndexProperty(this);
    }

    JsonNode toNotifyJson() {
        ObjectNode propertyObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        propertyObjectNode.put("name", this.name);
        propertyObjectNode.put("propertyType", this.propertyType.name());
        return propertyObjectNode;
    }

    static PropertyColumn fromNotifyJson(AbstractLabel abstractLabel, JsonNode jsonNode) {
        PropertyColumn property = new PropertyColumn(
                abstractLabel,
                jsonNode.get("name").asText(),
                PropertyType.valueOf(jsonNode.get("propertyType").asText()));
        return property;
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
        if (this.abstractLabel.getSchema().equals(other.abstractLabel.getSchema())) {
            if (this.abstractLabel.equals(other.abstractLabel)) {
                return this.getName().equals(other.getName()) && this.getPropertyType() == other.getPropertyType();
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
}
