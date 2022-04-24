package org.umlg.sqlg.structure.topology;

import org.umlg.sqlg.structure.Multiplicity;

public record EdgeDefinition(Multiplicity inMultiplicity, Multiplicity outMultiplicity) {

    public static EdgeDefinition of() {
        return new EdgeDefinition(Multiplicity.from(0, -1), Multiplicity.from(0, -1));
    }

}
