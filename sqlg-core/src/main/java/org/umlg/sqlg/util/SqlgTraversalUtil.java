package org.umlg.sqlg.util;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.LambdaFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaCollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.LambdaSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.umlg.sqlg.structure.traverser.SqlgTraverserGenerator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/04/29
 */
public class SqlgTraversalUtil {

    public static <S, E> boolean test(final Traverser.Admin<S> traverser, final Traversal.Admin<S, E> traversal) {
        final Traverser.Admin<S> split = traverser.split();
        split.setSideEffects(traversal.getSideEffects());
        split.setBulk(1l);
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

    public static <S> S lastLambdaHolderBefore(final Traversal.Admin<?, ?> traversal, Step<?,?> step) {
        List<Class<? extends Step>> lambdaHolders = Arrays.asList(
                SackValueStep.class,
                LambdaFilterStep.class,
                LambdaMapStep.class,
                LambdaFlatMapStep.class,
                LambdaCollectingBarrierStep.class,
                LambdaSideEffectStep.class
        );
        for (Class<? extends Step> lambdaHolder : lambdaHolders) {
            int index = TraversalHelper.stepIndex(step, traversal);
            List<S> steps = TraversalHelper.getStepsOfAssignableClassRecursively((Class<S>)lambdaHolder, traversal);
            Collections.reverse(steps);
            for (S s : steps) {
                int stepIndex = TraversalHelper.stepIndex((Step)s, traversal);
                if (stepIndex < index) {
                    return s;
                }
            }
        }
        return null;
    }

    public static <S> S stepAfter(final Traversal.Admin<?, ?> traversal, final Class<S> stepClass, Step<?,?> step) {
        int index = TraversalHelper.stepIndex(step, traversal);
        List<S> steps = TraversalHelper.getStepsOfAssignableClass(stepClass, traversal);
        Collections.reverse(steps);
        for (S s : steps) {
            int stepIndex = TraversalHelper.stepIndex((Step)s, traversal);
            if (stepIndex > index) {
                return s;
            }
        }
        return null;
    }

    public static boolean mayOptimize(final Traversal.Admin<?, ?> traversal) {
        return true;
//        return !TraversalHelper.hasStepOfAssignableClass(FoldStep.class, traversal);
    }

    public static final <S, E> boolean test(final S start, final Traversal.Admin<S, E> traversal) {
        traversal.reset();
        Traverser.Admin<S> admin = SqlgTraverserGenerator.instance().generate(
                start,
                traversal.getStartStep(),
                1L, false, false);
        traversal.addStart(admin);
        boolean result = traversal.hasNext(); // filter
        //Close the traversal to release any underlying resources.
        CloseableIterator.closeIterator(traversal);
        return result;
    }
}
