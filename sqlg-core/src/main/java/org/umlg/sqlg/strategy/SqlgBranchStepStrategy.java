package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.structure.SqlgGraph;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2014/08/15
 */
public class SqlgBranchStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy  {


    public SqlgBranchStepStrategy() {
        super();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        //Only optimize SqlgGraph. StarGraph also passes through here.
        //noinspection OptionalGetWithoutIsPresent
        if (!(traversal.getGraph().get() instanceof SqlgGraph)) {
            return;
        }
        List<ChooseStep> branchStep = TraversalHelper.getStepsOfAssignableClassRecursively(ChooseStep.class, traversal);
        for (BranchStep step : branchStep) {
            try {
                Field barrierField = step.getClass().getSuperclass().getDeclaredField("hasBarrier");
                barrierField.setAccessible(true);
                barrierField.set(step, true);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPost() {
        return Stream.of(
                SqlgGraphStepStrategy.class
        ).collect(Collectors.toSet());
    }

}
