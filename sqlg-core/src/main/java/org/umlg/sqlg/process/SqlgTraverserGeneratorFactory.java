package org.umlg.sqlg.process;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserGeneratorFactory;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.Set;

/**
 * Created by pieter on 2015/07/20.
 */
public class SqlgTraverserGeneratorFactory implements TraverserGeneratorFactory {

    @Override
    public TraverserGenerator getTraverserGenerator(Traversal.Admin<?, ?> traversal) {
        final Set<TraverserRequirement> requirements = traversal.getTraverserRequirements();
        if (SqlgGraphStepTraverserGenerator.instance().getProvidedRequirements().containsAll(requirements)) {
            return SqlgGraphStepTraverserGenerator.instance();
        } else {
            return SqlgGraphStepWithPathTraverserGenerator.instance();
        }
    }
}
