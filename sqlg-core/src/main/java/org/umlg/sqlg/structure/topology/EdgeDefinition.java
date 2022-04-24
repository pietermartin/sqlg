package org.umlg.sqlg.structure.topology;

import org.umlg.sqlg.structure.Multiplicity;

public record EdgeDefinition(Multiplicity outMultiplicity, Multiplicity inMultiplicity) {

    public static EdgeDefinition of() {
        return new EdgeDefinition(Multiplicity.from(0, -1), Multiplicity.from(0, -1));
    }

}
