package org.umlg.sqlg.util;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;
import java.util.function.Predicate;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/04/29
 */
public class SqlgTraversalUtil {

    public static <S, E> boolean test(final Traverser.Admin<S> traverser, final Traversal.Admin<S, E> traversal) {
        final Traverser.Admin<S> split = traverser.split();
        split.setSideEffects(traversal.getSideEffects());
        split.setBulk(1l);
//        traversal.reset();
        traversal.addStart(split);
        return traversal.hasNext(); // filter
    }

    public static boolean hasOneBulkRequirement(Traversal.Admin<?, ?> traversal) {
        List<Step> steps = TraversalHelper.getStepsOfAssignableClassRecursively(Step.class, traversal);
        for (Step step : steps) {
            if (step.getRequirements().contains(TraverserRequirement.ONE_BULK)) {
                return true;
            }
        }
        return false;
    }

    public static boolean anyStepRecursively(final Predicate<Step> predicate, final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (predicate.test(step)) {
                return true;
            }

            if (step instanceof TraversalParent) {
                if (anyStepRecursively(predicate, ((TraversalParent) step))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean anyStepRecursively(final Predicate<Step> predicate, final TraversalParent step) {
        for (final Traversal.Admin<?, ?> localChild : step.getLocalChildren()) {
            if (anyStepRecursively(predicate, localChild)) return true;
        }
        for (final Traversal.Admin<?, ?> globalChild : step.getGlobalChildren()) {
            if (anyStepRecursively(predicate, globalChild)) return true;
        }
        return false;
    }
}
