package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.sql.parse.ReplacedStep;

import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.function.BiPredicate;

/**
 * Created by pieter on 2015/07/19.
 */
public abstract class BaseSqlgStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final List<BiPredicate> SUPPORTED_BI_PREDICATE = Arrays.asList(Compare.eq);

    protected boolean mayNotBeOptimized(List<Step> steps, int index) {
        List<Step> toCome = steps.subList(index, steps.size());
        return toCome.stream().anyMatch(s ->
                s.getClass().equals(PathStep.class) ||
                        s.getClass().equals(TreeStep.class) ||
                        s.getClass().equals(TreeSideEffectStep.class) ||
                        s.getClass().equals(Order.class));
    }

    protected void collectHasSteps(ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> replacedStep) {
        //Collect the hasSteps
        while (iterator.hasNext()) {
            Step<?, ?> currentStep = iterator.next();
            if (currentStep instanceof HasContainerHolder && SUPPORTED_BI_PREDICATE.contains(((HasContainerHolder) currentStep).getHasContainers().get(0).getBiPredicate())) {
                if (!currentStep.getLabels().isEmpty()) {
                    final IdentityStep identityStep = new IdentityStep<>(traversal);
                    currentStep.getLabels().forEach(replacedStep::addLabel);
                    TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                }
                iterator.remove();
                traversal.removeStep(currentStep);
                replacedStep.getHasContainers().addAll(((HasContainerHolder) currentStep).getHasContainers());
            } else if (currentStep instanceof IdentityStep) {
                // do nothing
            } else {
                iterator.previous();
                break;
            }
        }
    }
}
