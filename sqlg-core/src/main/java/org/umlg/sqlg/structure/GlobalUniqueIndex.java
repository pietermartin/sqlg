package org.umlg.sqlg.structure;

import java.util.*;

/**
 * Date: 2016/12/01
 * Time: 10:10 PM
 */
public class GlobalUniqueIndex implements TopologyInf {

    private Topology topology;
    private String name;
    private Set<PropertyColumn> properties = new HashSet<>();
    private Set<PropertyColumn> uncommittedProperties = new HashSet<>();
    public static final String GLOBAL_UNIQUE_INDEX_VALUE = "value";
    public static final String GLOBAL_UNIQUE_INDEX_RECORD_ID = "recordId";
    public static final String GLOBAL_UNIQUE_INDEX_PROPERTY_NAME = "property";

    public GlobalUniqueIndex(Topology topology, String name, Set<PropertyColumn> uncommittedProperties) {
        this.topology = topology;
        this.name = name;
        this.uncommittedProperties = uncommittedProperties;
        for (PropertyColumn uncommittedProperty : uncommittedProperties) {
            uncommittedProperty.addGlobalUniqueIndex(this);
        }
    }

    public String getName() {
        return name;
    }

    void afterCommit() {
        if (this.topology.isWriteLockHeldByCurrentThread()) {
            Iterator<PropertyColumn> propertyColumnIterator = this.uncommittedProperties.iterator();
            while (propertyColumnIterator.hasNext()) {
                PropertyColumn propertyColumn = propertyColumnIterator.next();
                this.properties.add(propertyColumn);
                propertyColumnIterator.remove();
            }
        }
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
        VertexLabel vertexLabel = topology.getGlobalUniqueIndexSchema().ensureVertexLabelExist(sqlgGraph, globalUniqueIndexName, valueColumn);
        PropertyColumn valuePropertyColumn = vertexLabel.getProperty(GLOBAL_UNIQUE_INDEX_VALUE).get();
        PropertyColumn recordIdColumn = vertexLabel.getProperty(GLOBAL_UNIQUE_INDEX_RECORD_ID).get();
        PropertyColumn propertyColumn = vertexLabel.getProperty(GLOBAL_UNIQUE_INDEX_PROPERTY_NAME).get();
        vertexLabel.ensureIndexExists(sqlgGraph, IndexType.UNIQUE, Collections.singletonList(valuePropertyColumn));
        vertexLabel.ensureIndexExists(sqlgGraph, IndexType.UNIQUE, Arrays.asList(recordIdColumn, propertyColumn));
        GlobalUniqueIndex globalUniqueIndex = new GlobalUniqueIndex(topology, globalUniqueIndexName, properties);
        topology.addToGlobalUniqueIndexes(globalUniqueIndex);
        TopologyManager.addGlobalUniqueIndex(sqlgGraph, globalUniqueIndexName, properties);
        return globalUniqueIndex;
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    static String globalUniqueIndexName(Set<PropertyColumn> properties) {
        List<PropertyColumn> propertyColumns = new ArrayList<>(properties);
        propertyColumns.sort(Comparator.comparing(PropertyColumn::getName));
        return propertyColumns.stream().map(p->p.getAbstractLabel().getLabel() + "_" + p.getName()).reduce((a, b) -> a + "_" + b).get();
    }
}
