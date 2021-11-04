package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.step.SqlgInjectStep;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.List;

public class SqlgInjectStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    public SqlgInjectStepStrategy() {
        super();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        //Only optimize SqlgGraph. StarGraph also passes through here.
        if (traversal.getGraph().isEmpty() || !(traversal.getGraph().orElseThrow(IllegalStateException::new) instanceof SqlgGraph)) {
            return;
        }
        List<InjectStep> injectSteps = TraversalHelper.getStepsOfAssignableClass(InjectStep.class, traversal);
        for (InjectStep<?> injectStep : injectSteps) {
            SqlgInjectStep sqlgInjectStep = new SqlgInjectStep(traversal, injectStep.getInjections());
            for (String label : injectStep.getLabels()) {
                sqlgInjectStep.addLabel(label);
            }
            //noinspection unchecked
            TraversalHelper.replaceStep(
                    injectStep,
                    sqlgInjectStep,
                    injectStep.getTraversal()
            );
        }
    }

//    @Override
//    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
//        return Stream.of(
//        ).collect(Collectors.toSet());
//    }
}
