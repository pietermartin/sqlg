package org.umlg.sqlg.topology;

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
}
