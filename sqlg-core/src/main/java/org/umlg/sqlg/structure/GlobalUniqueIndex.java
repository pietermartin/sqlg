package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

import java.time.LocalDateTime;
import java.util.*;

/**
 * Date: 2016/12/01
 * Time: 10:10 PM
 */
public class GlobalUniqueIndex implements TopologyInf {

    private Topology topology;
    private String name;
    private boolean committed = true;
    private Set<PropertyColumn> properties = new HashSet<>();
    private Set<PropertyColumn> uncommittedProperties = new HashSet<>();
    public static final String GLOBAL_UNIQUE_INDEX_VALUE = "value";
    public static final String GLOBAL_UNIQUE_INDEX_RECORD_ID = "recordId";
    public static final String GLOBAL_UNIQUE_INDEX_PROPERTY_NAME = "property";

    private GlobalUniqueIndex(Topology topology, String name, Set<PropertyColumn> uncommittedProperties) {
        this.topology = topology;
        this.name = name;
        this.uncommittedProperties = uncommittedProperties;
        for (PropertyColumn uncommittedProperty : uncommittedProperties) {
            uncommittedProperty.addGlobalUniqueIndex(this);
        }
    }

    public Set<PropertyColumn> getProperties() {
        return properties;
    }

    private GlobalUniqueIndex(Topology topology, String name) {
        this.topology = topology;
        this.name = name;
    }

    static GlobalUniqueIndex instantiateGlobalUniqueIndex(Topology topology, String name) {
        return new GlobalUniqueIndex(topology, name);
    }

    public String getName() {
        return name;
    }
    
    @Override
    public int hashCode() {
        return this.getName().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) {
            return true;
        }
        if (!(other instanceof GlobalUniqueIndex)) {
            return false;
        }
        GlobalUniqueIndex otherIndex = (GlobalUniqueIndex) other;
        return this.name.equals(otherIndex.name);
    }

    @Override
    public boolean isCommitted() {
        return this.committed;
    }

    /**
     * Called from {@link Topology#fromNotifyJson(int, LocalDateTime)}
     *
     * @param properties The properties.
     */
    void addGlobalUniqueProperties(Set<PropertyColumn> properties) {
        for (PropertyColumn property : properties) {
            property.addToGlobalUniqueIndexes(this);
        }
        this.properties = properties;
    }

    void afterCommit() {
        Iterator<PropertyColumn> propertyColumnIterator = this.uncommittedProperties.iterator();
        while (propertyColumnIterator.hasNext()) {
            PropertyColumn propertyColumn = propertyColumnIterator.next();
            this.properties.add(propertyColumn);
            propertyColumnIterator.remove();
        }
        this.committed = true;
    }

    void afterRollback() {
        this.uncommittedProperties.clear();
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    static GlobalUniqueIndex createGlobalUniqueIndex(SqlgGraph sqlgGraph, Topology topology, String globalUniqueIndexName, Set<PropertyColumn> properties) {
        //all PropertyColumns must be for the same PropertyType
        PropertyType propertyType = properties.iterator().next().getPropertyType();
        Map<String, PropertyType> valueColumn = new HashMap<>();
        valueColumn.put(GLOBAL_UNIQUE_INDEX_RECORD_ID, PropertyType.STRING);
        valueColumn.put(GLOBAL_UNIQUE_INDEX_PROPERTY_NAME, PropertyType.STRING);
        valueColumn.put(GLOBAL_UNIQUE_INDEX_VALUE, propertyType);
        VertexLabel vertexLabel = topology.getGlobalUniqueIndexSchema().ensureVertexLabelExist(globalUniqueIndexName, valueColumn);
        PropertyColumn valuePropertyColumn = vertexLabel.getProperty(GLOBAL_UNIQUE_INDEX_VALUE).get();
        PropertyColumn recordIdColumn = vertexLabel.getProperty(GLOBAL_UNIQUE_INDEX_RECORD_ID).get();
        PropertyColumn propertyColumn = vertexLabel.getProperty(GLOBAL_UNIQUE_INDEX_PROPERTY_NAME).get();
        vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(valuePropertyColumn));
        vertexLabel.ensureIndexExists(IndexType.UNIQUE, Arrays.asList(recordIdColumn, propertyColumn));
        GlobalUniqueIndex globalUniqueIndex = new GlobalUniqueIndex(topology, globalUniqueIndexName, properties);
        topology.getGlobalUniqueIndexSchema().globalUniqueIndexes.put(globalUniqueIndex.getName(),globalUniqueIndex);
        TopologyManager.addGlobalUniqueIndex(sqlgGraph, globalUniqueIndexName, properties);
        globalUniqueIndex.committed = false;
        return globalUniqueIndex;
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    static String globalUniqueIndexName(Topology topology, Set<PropertyColumn> properties) {
        List<PropertyColumn> propertyColumns = new ArrayList<>(properties);
        propertyColumns.sort(Comparator.comparing(PropertyColumn::getName));
        String name = propertyColumns.stream().map(p -> p.getParentLabel().getLabel() + "_" + p.getName()).reduce((a, b) -> a + "_" + b).get();
        if (("gui_schema_V_A" + name).length() > 50) {
            name = "globalUniqueIndex_" + topology.getGlobalUniqueIndexes().size();
        }
        return name;
    }

    Optional<JsonNode> toNotifyJson() {
        Preconditions.checkState(this.topology.isWriteLockHeldByCurrentThread(), "GlobalUniqueIndex toNotifyJson() may only be called is the lock is held.");
        ObjectNode result = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        ArrayNode propertyArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (PropertyColumn property : this.uncommittedProperties) {
            ObjectNode objectNode = property.toNotifyJson();
            objectNode.put("schemaName", property.getParentLabel().getSchema().getName());
            objectNode.put("abstractLabelLabel", property.getParentLabel().getLabel());
            propertyArrayNode.add(objectNode);
        }
        result.put("name", getName());
        result.set("uncommittedProperties", propertyArrayNode);
        return Optional.of(result);
    }

    /**
     * JSON representation of committed state
     * @return
     */
    protected JsonNode toJson(){
    	ObjectNode result = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        ArrayNode propertyArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (PropertyColumn property : this.properties) {
            ObjectNode objectNode = property.toNotifyJson();
            objectNode.put("schemaName", property.getParentLabel().getSchema().getName());
            objectNode.put("abstractLabelLabel", property.getParentLabel().getLabel());
            propertyArrayNode.add(objectNode);
        }
        result.put("name", getName());
        result.set("properties", propertyArrayNode);
        return result;
    }
    
    @Override
    public String toString() {
        return toJson().toString();
    }
    
    
    @Override
    public void remove(boolean preserveData) {
    	topology.getGlobalUniqueIndexSchema().removeGlobalUniqueIndex(this,preserveData);
    }
}
