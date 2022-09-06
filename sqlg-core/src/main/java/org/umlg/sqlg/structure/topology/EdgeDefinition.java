package org.umlg.sqlg.structure.topology;

import org.umlg.sqlg.structure.Multiplicity;

public record EdgeDefinition(Multiplicity outMultiplicity, Multiplicity inMultiplicity) {

    public static EdgeDefinition of(Multiplicity outMultiplicity, Multiplicity inMultiplicity) {
        return new EdgeDefinition(outMultiplicity, inMultiplicity);
    }

    public static EdgeDefinition of() {
        return new EdgeDefinition(Multiplicity.of(0, -1), Multiplicity.of(0, -1));
    }

}
