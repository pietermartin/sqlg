package org.umlg.sqlg.process;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.*;

import java.util.Set;

/**
 * Created by pieter on 2015/07/20.
 */
public class SqlgTraverserGeneratorFactory implements TraverserGeneratorFactory {

    @Override
    public TraverserGenerator getTraverserGenerator(Traversal.Admin<?, ?> traversal) {
        if (traversal.getTraverserRequirements().contains(TraverserRequirement.ONE_BULK) || traversal.getTraverserRequirements().contains(TraverserRequirement.SACK)) {
            return tpTraverserGenerator(traversal);
        } else {
            return SqlgGraphStepWithPathTraverserGenerator.instance();
        }
    }

    private TraverserGenerator tpTraverserGenerator(final Traversal.Admin<?, ?> traversal) {
        final Set<TraverserRequirement> requirements = traversal.getTraverserRequirements();

        if (requirements.contains(TraverserRequirement.ONE_BULK)) {
            if (O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return O_OB_S_SE_SL_TraverserGenerator.instance();

            if (LP_O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return LP_O_OB_S_SE_SL_TraverserGenerator.instance();

            if (LP_O_OB_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return LP_O_OB_P_S_SE_SL_TraverserGenerator.instance();
        } else {
            if (O_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return O_TraverserGenerator.instance();

            if (B_O_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_O_TraverserGenerator.instance();

            if (B_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_O_S_SE_SL_TraverserGenerator.instance();

            if (B_LP_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_LP_O_S_SE_SL_TraverserGenerator.instance();

            if (B_LP_O_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_LP_O_P_S_SE_SL_TraverserGenerator.instance();
        }

        throw new IllegalStateException("The provided traverser generator factory does not support the requirements of the traversal: " + this.getClass().getCanonicalName() + requirements);
    }
}
