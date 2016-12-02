package org.umlg.sqlg.structure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Date: 2016/12/01
 * Time: 10:10 PM
 */
public class GlobalIndex {

    private Logger logger = LoggerFactory.getLogger(GlobalIndex.class.getName());
    private Map<String, PropertyColumn> properties = new HashMap<>();
    private Map<String, PropertyColumn> uncommittedProperties = new HashMap<>();

    public static GlobalIndex ensureGlobalIndexExist(SqlgGraph sqlgGraph, Set<PropertyColumn> properties) {
        Objects.requireNonNull(properties, "properties may not be null");
        PropertyColumn propertyColumn1 = properties.iterator().next();

        Optional<GlobalIndex> globalIndexOptional = getGlobalIndex(properties);
        if (!globalIndexOptional.isPresent()) {
            //take any property
            properties.iterator().next().getAbstractLabel().getSchema().getTopology().lock();
            globalIndexOptional = this.getGlobalIndex(properties);
            if (!globalIndexOptional.isPresent()) {
                return this.createCreateGlobalIndex(sqlgGraph, properties);
            } else {
                return globalIndexOptional.get();
            }
        } else {
            return globalIndexOptional.get();
        }
        return null;
    }
}
