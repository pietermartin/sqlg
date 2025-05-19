package org.umlg.sqlg.sql.parse;

import org.umlg.sqlg.structure.PropertyDefinition;

import java.util.function.Function;

public record SqlgFunctionConfig(String col, PropertyDefinition propertyDefinition, Function<Object, String> function) {

}
