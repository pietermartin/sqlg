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

    static GlobalUniqueIndex createGlobalUniqueIndex(SqlgGraph sqlgGraph, Topology topology, String globalUniqueIndexName, Set<PropertyColumn> properties) {
        //all PropertyColumns must be for the same PropertyType
        PropertyType propertyType = properties.iterator().next().getPropertyType();
        Map<String, PropertyType> valueColumn = new HashMap<>();
        valueColumn.put(GLOBAL_UNIQUE_INDEX_VALUE, propertyType);
        VertexLabel vertexLabel = topology.getGlobalUniqueIndexSchema().ensureVertexLabelExist(sqlgGraph, globalUniqueIndexName, valueColumn);
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        PropertyColumn valuePropertyColumn = vertexLabel.getProperty(GLOBAL_UNIQUE_INDEX_VALUE).get();
        vertexLabel.ensureIndexExists(sqlgGraph, IndexType.UNIQUE, Collections.singletonList(valuePropertyColumn));
        GlobalUniqueIndex globalUniqueIndex = new GlobalUniqueIndex(topology, globalUniqueIndexName, properties);
        topology.addToGlobalUniqueIndexes(globalUniqueIndex);
        TopologyManager.addGlobalUniqueIndex(sqlgGraph, globalUniqueIndexName, properties);
        return globalUniqueIndex;
    }

    static String globalUniqueIndexName(Set<PropertyColumn> properties) {
        List<PropertyColumn> propertyColumns = new ArrayList<>(properties);
        Collections.sort(propertyColumns, Comparator.comparing(PropertyColumn::getName));
        //noinspection OptionalGetWithoutIsPresent
        return propertyColumns.stream().map(PropertyColumn::getName).reduce((a, b) -> a + "_" + b).get();
    }
}
