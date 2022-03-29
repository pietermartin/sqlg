package org.umlg.sqlg.structure;

public record PropertyDefinition(PropertyType propertyType, Multiplicity multiplicity, String defaultLiteral, String checkConstraint) {
    public PropertyDefinition {
        if (!propertyType.isArray() && multiplicity.isMany()) {
            throw new IllegalArgumentException("Multiplicity can only be a many for array types.");
        }
    }

    public PropertyDefinition(PropertyType propertyType) {
        this(propertyType, (propertyType.isArray() ? new Multiplicity(0, -1) : new Multiplicity()), null, null);
    }

    public static PropertyDefinition of(PropertyType propertyType) {
        return new PropertyDefinition(propertyType);
    }
}
