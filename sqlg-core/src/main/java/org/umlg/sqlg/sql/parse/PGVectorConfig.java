package org.umlg.sqlg.sql.parse;

import org.umlg.sqlg.util.Preconditions;
import org.umlg.sqlg.services.SqlgPGVectorFactory;
import org.umlg.sqlg.structure.topology.AbstractLabel;

public record PGVectorConfig(String name, String source, float[] target, AbstractLabel abstractLabel) {

    public PGVectorConfig(String name, String source, float[] target, AbstractLabel abstractLabel) {
        this.name = name;
        this.source = source;
        this.target = target;
        this.abstractLabel = abstractLabel;
        Preconditions.checkState(name.equals(SqlgPGVectorFactory.l2distance));
        Preconditions.checkState(abstractLabel.getProperty(source).isPresent(), "source %s is not a property of %s", source, abstractLabel.getName());
        Preconditions.checkState(target.length != 0, "target vector may not be empty");
    }

}
