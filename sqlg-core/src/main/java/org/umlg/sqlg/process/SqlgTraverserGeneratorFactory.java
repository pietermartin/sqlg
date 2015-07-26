package org.umlg.sqlg.process;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserGeneratorFactory;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.DefaultTraverserGeneratorFactory;
import org.umlg.sqlg.strategy.SqlgGraphStepCompiled;

/**
 * Created by pieter on 2015/07/20.
 */
public class SqlgTraverserGeneratorFactory implements TraverserGeneratorFactory {

    @Override
    public TraverserGenerator getTraverserGenerator(Traversal.Admin<?, ?> traversal) {
        //TODO ticket on tp3
//        final Set<TraverserRequirement> requirements = traversal.getTraverserRequirements();
//        if (SqlgGraphStepTraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
//            return SqlgGraphStepTraverserGenerator.instance();

        TraverserGenerator traverserGenerator = DefaultTraverserGeneratorFactory.instance().getTraverserGenerator(traversal);
        if (traversal.getStartStep() instanceof SqlgGraphStepCompiled) {
            if (traverserGenerator instanceof B_LP_O_P_S_SE_SL_TraverserGenerator) {
                return SqlgGraphStepWithPathTraverserGenerator.instance();
            } else {
                return SqlgGraphStepTraverserGenerator.instance();
            }
        } else {
            return traverserGenerator;
        }

    }
}
