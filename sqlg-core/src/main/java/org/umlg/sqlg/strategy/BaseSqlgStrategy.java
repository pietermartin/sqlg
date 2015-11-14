package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.TraversalComparator;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;
import org.umlg.sqlg.predicate.Text;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.structure.SqlgGraph;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * Created by pieter on 2015/07/19.
 */
public abstract class BaseSqlgStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    protected SqlgGraph sqlgGraph;
    public static final String PATH_LABEL_SUFFIX = "P~~~";
    public static final String EMIT_LABEL_SUFFIX = "E~~~";
    public static final String SQLG_PATH_FAKE_LABEL = "sqlgPathFakeLabel";
    private static final List<BiPredicate> SUPPORTED_BI_PREDICATE = Arrays.asList(
            Compare.eq, Compare.neq, Compare.gt, Compare.gte, Compare.lt, Compare.lte);

    public BaseSqlgStrategy(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
    }

    protected abstract void handleFirstReplacedStep(Step firstStep, SqlgStep sqlgStep, Traversal.Admin<?, ?> traversal);

    protected abstract boolean doLastEntry(Step step, ListIterator<Step> stepIterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> lastReplacedStep, SqlgStep sqlgStep);

    protected abstract boolean isReplaceableStep(Class<? extends Step> stepClass);

    protected abstract SqlgStep constructSqlgStep(Traversal.Admin<?, ?> traversal, Step startStep);

    protected boolean unoptimizableRepeat(List<Step> steps, int index) {
        List<Step> toCome = steps.subList(index, steps.size());
        boolean repeatExist = toCome.stream().anyMatch(s -> s.getClass().equals(RepeatStep.class));
        if (repeatExist) {
            //TODO tp3 3.1.0-incubating
            Class repeatStepClass;
            try {
                repeatStepClass = Class.forName("org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            boolean hasUntil = toCome.stream().filter(s -> s.getClass().equals(RepeatStep.class)).allMatch(r -> {
                try {
                    Field untilTraversalField = repeatStepClass.getDeclaredField("untilTraversal");
                    untilTraversalField.setAccessible(true);
                    return untilTraversalField.get(r) != null;
                } catch (NoSuchFieldException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
                //TODO tp3 3.1.0-incubating
//                ((RepeatStep) r).getUntilTraversal() != null
            });
            boolean hasUnoptimizableUntil = false;
            if (hasUntil) {
                hasUnoptimizableUntil = toCome.stream().filter(s -> s.getClass().equals(RepeatStep.class)).allMatch(r -> {
                    try {
                        Field untilTraversalField = repeatStepClass.getDeclaredField("untilTraversal");
                        untilTraversalField.setAccessible(true);
                        return !(untilTraversalField.get(r)  instanceof LoopTraversal);
                    } catch (NoSuchFieldException e) {
                        throw new RuntimeException(e);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    //TODO tp3 3.1.0-incubating
//                    return !(((RepeatStep) r).getUntilTraversal() instanceof LoopTraversal)
                });
            }
            boolean badRepeat = !hasUntil || hasUnoptimizableUntil;
            //Check if the repeat step only contains optimizable steps
            if (!badRepeat) {
                List<Step> collectedRepeatInternalSteps = new ArrayList<>();
                List<Step> repeatSteps = toCome.stream().filter(s -> s.getClass().equals(RepeatStep.class)).collect(Collectors.toList());
                for (Step step : repeatSteps) {
                    RepeatStep repeatStep = (RepeatStep) step;
                    List<Traversal.Admin> repeatTraversals = repeatStep.<Traversal.Admin>getGlobalChildren();
                    Traversal.Admin admin = repeatTraversals.get(0);
                    List<Step> repeatInternalSteps = admin.getSteps();
                    collectedRepeatInternalSteps.addAll(repeatInternalSteps);
                }
                return !collectedRepeatInternalSteps.stream().filter(s -> !s.getClass().equals(RepeatStep.RepeatEndStep.class))
                        .allMatch((s) -> isReplaceableStep(s.getClass()));
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    protected boolean canNotBeOptimized(List<Step> steps, int index) {
        List<Step> toCome = steps.subList(index, steps.size());
        return toCome.stream().anyMatch(s ->
                s.getClass().equals(Order.class));
    }

    protected boolean precedesPathOrTreeStep(List<Step> steps, int index) {
        List<Step> toCome = steps.subList(index, steps.size());
        return toCome.stream().anyMatch(s ->
                (s.getClass().equals(PathStep.class) ||
                        s.getClass().equals(TreeStep.class) ||
                        s.getClass().equals(TreeSideEffectStep.class)));
    }

    protected void collectHasSteps(ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> replacedStep, int pathCount) {
        //Collect the hasSteps
        while (iterator.hasNext()) {
            Step<?, ?> currentStep = iterator.next();
            if (currentStep instanceof HasContainerHolder && isNotZonedDateTimeOrPeriodOrDuration((HasContainerHolder) currentStep) &&
                    (isSingleBiPredicate(((HasContainerHolder) currentStep).getHasContainers()) ||
                            isBetween(((HasContainerHolder) currentStep).getHasContainers()) ||
                            isInside(((HasContainerHolder) currentStep).getHasContainers()) ||
                            isOutside(((HasContainerHolder) currentStep).getHasContainers()) ||
                            isWithinOut(((HasContainerHolder) currentStep).getHasContainers()) ||
                            isTextContains(((HasContainerHolder) currentStep).getHasContainers()))) {
                if (!currentStep.getLabels().isEmpty()) {
                    final IdentityStep identityStep = new IdentityStep<>(traversal);
                    currentStep.getLabels().forEach(l -> replacedStep.addLabel(pathCount + BaseSqlgStrategy.PATH_LABEL_SUFFIX + l));
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

    protected boolean isNotZonedDateTimeOrPeriodOrDuration(HasContainerHolder currentStep) {
        return currentStep.getHasContainers().stream().filter(
                h -> h.getPredicate().getValue() instanceof ZonedDateTime ||
                        h.getPredicate().getValue() instanceof Period ||
                        h.getPredicate().getValue() instanceof Duration ||
                        (h.getPredicate().getValue() instanceof List && (
                                ((List) h.getPredicate().getValue()).stream().anyMatch(v -> v instanceof ZonedDateTime) ||
                                        ((List) h.getPredicate().getValue()).stream().anyMatch(v -> v instanceof Period) ||
                                        ((List) h.getPredicate().getValue()).stream().anyMatch(v -> v instanceof Duration)))
        ).count() < 1;
    }

    static void collectOrderGlobalSteps(Step step, ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> replacedStep) {
        //Collect the OrderGlobalSteps
        if (step instanceof OrderGlobalStep && isElementValueComparator((OrderGlobalStep) step)) {
            iterator.remove();
            traversal.removeStep(step);
            replacedStep.getComparators().addAll(((OrderGlobalStep) step).getComparators());
        } else {
            collectSelectOrderGlobalSteps(iterator, traversal, replacedStep);
        }
    }

    private static void collectSelectOrderGlobalSteps(ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> replacedStep) {
        //Collect the OrderGlobalSteps
        while (iterator.hasNext()) {
            Step<?, ?> currentStep = iterator.next();
            if (currentStep instanceof OrderGlobalStep && isElementValueComparator((OrderGlobalStep) currentStep)) {
                iterator.remove();
                traversal.removeStep(currentStep);
                replacedStep.getComparators().addAll(((OrderGlobalStep) currentStep).getComparators());
            } else if (currentStep instanceof OrderGlobalStep && isTraversalComparatorWithSelectOneStep((OrderGlobalStep) currentStep)) {
                iterator.remove();
                traversal.removeStep(currentStep);
                replacedStep.getComparators().addAll(((OrderGlobalStep) currentStep).getComparators());
            } else if (currentStep instanceof IdentityStep) {
                // do nothing
            } else {
                iterator.previous();
                break;
            }
        }
    }

    static boolean isElementValueComparator(OrderGlobalStep orderGlobalStep) {
        return orderGlobalStep.getComparators().stream().allMatch(c -> c instanceof ElementValueComparator
                && (((ElementValueComparator) c).getValueComparator() == Order.incr ||
                ((ElementValueComparator) c).getValueComparator() == Order.decr));
    }

    static boolean isTraversalComparatorWithSelectOneStep(OrderGlobalStep orderGlobalStep) {
        for (Object o : orderGlobalStep.getComparators()) {
            if (o instanceof TraversalComparator) {
                TraversalComparator traversalComparator = (TraversalComparator) o;
                List<Step> traversalComparatorSteps = traversalComparator.getTraversal().getSteps();
                return traversalComparatorSteps.size() == 1 && traversalComparatorSteps.get(0) instanceof SelectOneStep;
            } else {
                return false;
            }
        }
        if (orderGlobalStep.getComparators().stream().allMatch(c -> c instanceof TraversalComparator)) {
        } else {
            return false;
        }
        return orderGlobalStep.getComparators().stream().allMatch(c -> c instanceof TraversalComparator
                && (((ElementValueComparator) c).getValueComparator() == Order.incr ||
                ((ElementValueComparator) c).getValueComparator() == Order.decr));
    }

    private boolean isSingleBiPredicate(List<HasContainer> hasContainers) {
        if (hasContainers.size() == 1) {
            return SUPPORTED_BI_PREDICATE.contains(hasContainers.get(0).getBiPredicate());
        } else {
            return false;
        }
    }

    private boolean isBetween(List<HasContainer> hasContainers) {
        if (hasContainers.size() == 2) {
            HasContainer hasContainer1 = hasContainers.get(0);
            HasContainer hasContainer2 = hasContainers.get(1);
            return hasContainer1.getBiPredicate().equals(Compare.gte) && hasContainer2.getBiPredicate().equals(Compare.lt);
        } else {
            return false;
        }
    }

    private boolean isInside(List<HasContainer> hasContainers) {
        if (hasContainers.size() == 2) {
            HasContainer hasContainer1 = hasContainers.get(0);
            HasContainer hasContainer2 = hasContainers.get(1);
            return hasContainer1.getBiPredicate().equals(Compare.gt) && hasContainer2.getBiPredicate().equals(Compare.lt);
        } else {
            return false;
        }
    }

    private <V> boolean isOutside(List<HasContainer> hasContainers) {
        if (hasContainers.size() == 1 && hasContainers.get(0).getPredicate() instanceof OrP) {
            OrP<V> orP = (OrP) hasContainers.get(0).getPredicate();
            if (orP.getPredicates().size() == 2) {
                P<V> predicate1 = orP.getPredicates().get(0);
                P<V> predicate2 = orP.getPredicates().get(1);
                return predicate1.getBiPredicate().equals(Compare.lt) && predicate2.getBiPredicate().equals(Compare.gt);
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    private boolean isWithinOut(List<HasContainer> hasContainers) {
        return (hasContainers.size() == 1 && !hasContainers.get(0).getKey().equals(T.label.getAccessor()) &&
                !hasContainers.get(0).getKey().equals(T.id.getAccessor()) &&
                (hasContainers.get(0).getBiPredicate() == Contains.without || hasContainers.get(0).getBiPredicate() == Contains.within));
    }

    private boolean isTextContains(List<HasContainer> hasContainers) {
        return (hasContainers.size() == 1 && !hasContainers.get(0).getKey().equals(T.label.getAccessor()) &&
                !hasContainers.get(0).getKey().equals(T.id.getAccessor()) &&
                (hasContainers.get(0).getBiPredicate() == Text.contains ||
                        hasContainers.get(0).getBiPredicate() == Text.ncontains ||
                        hasContainers.get(0).getBiPredicate() == Text.containsCIS ||
                        hasContainers.get(0).getBiPredicate() == Text.ncontainsCIS ||
                        hasContainers.get(0).getBiPredicate() == Text.startsWith ||
                        hasContainers.get(0).getBiPredicate() == Text.nstartsWith ||
                        hasContainers.get(0).getBiPredicate() == Text.endsWith ||
                        hasContainers.get(0).getBiPredicate() == Text.nendsWith
                ));
    }

    protected void babySitSteps(Traversal.Admin<?, ?> traversal, Step startStep, List<Step> steps, ListIterator<Step> stepIterator) {
        //Replace all consecutive VertexStep and HasStep with one step
        SqlgStep sqlgStep = null;
        Step previous = null;
        ReplacedStep<?, ?> lastReplacedStep = null;
        Class repeatStepClass;
        Class loopTraversalClass;
        try {
            repeatStepClass = Class.forName("org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep");
            loopTraversalClass = Class.forName("org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        int pathCount = 0;
        boolean repeatStepAdded = false;
        int repeatStepsAdded = 0;
        while (stepIterator.hasNext()) {
            Step step = stepIterator.next();

            //Check for RepeatStep(s) and insert them into the stepIterator
            if (step instanceof RepeatStep) {
                repeatStepsAdded = 0;
                repeatStepAdded = false;
                RepeatStep repeatStep = (RepeatStep) step;
                List<Traversal.Admin<?, ?>> repeatTraversals = repeatStep.getGlobalChildren();
                Traversal.Admin admin = repeatTraversals.get(0);
                List<Step> internalRepeatSteps = admin.getSteps();
                //this is guaranteed by the previous check unoptimizableRepeat(...)
                //TODO remove when go to 3.1.0-incubating
                LoopTraversal loopTraversal;
                long numberOfLoops;
                try {
                    Field untilTraversalField = repeatStepClass.getDeclaredField("untilTraversal");
                    untilTraversalField.setAccessible(true);
                    loopTraversal = (LoopTraversal) untilTraversalField.get(repeatStep);
                    Field maxLoopsField = loopTraversalClass.getDeclaredField("maxLoops");
                    maxLoopsField.setAccessible(true);
                    numberOfLoops = (Long) maxLoopsField.get(loopTraversal);
                } catch (NoSuchFieldException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
//                LoopTraversal loopTraversal = (LoopTraversal) repeatStep.getUntilTraversal();
//                long numberOfLoops = loopTraversal.getMaxLoops();

                //Bug on tp3, times after is the same as times before for now
                //A times(x) after is the same as a times(x + 1) before
                if (!repeatStep.untilFirst) {
                    numberOfLoops++;
                }
                for (int i = 0; i < numberOfLoops; i++) {
                    for (Step internalRepeatStep : internalRepeatSteps) {
                        if (internalRepeatStep instanceof RepeatStep.RepeatEndStep) {
                            break;
                        }
                        stepIterator.add(internalRepeatStep);
                        stepIterator.previous();
                        stepIterator.next();
                        repeatStepAdded = true;
                        repeatStepsAdded++;
                    }
                }
                traversal.removeStep(repeatStep);
                //this is needed for the stepIterator.next() to be the newly inserted steps
                for (int i = 0; i < repeatStepsAdded; i++) {
                    stepIterator.previous();
                }
            } else {

//                if (CONSECUTIVE_STEPS_TO_REPLACE.contains(step.getClass())) {
                if (isReplaceableStep(step.getClass())) {

                    //check if repeat steps were added to the stepIterator
                    boolean emit = false;
                    boolean emitFirst = false;
                    boolean untilFirst = false;
                    if (repeatStepsAdded > 0) {
                        repeatStepsAdded--;
                        RepeatStep repeatStep = (RepeatStep) step.getTraversal().getParent();
                        Field emitTraversalField;
                        try {
                            //TODO remove when go to 3.1.0-incubating
                            emitTraversalField = repeatStepClass.getDeclaredField("emitTraversal");
                            emitTraversalField.setAccessible(true);
                            emit = emitTraversalField.get(repeatStep) != null;
                        } catch (NoSuchFieldException e) {
                            throw new RuntimeException(e);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
//                        emit = repeatStep.getEmitTraversal() != null;
                        emitFirst = repeatStep.emitFirst;
                        untilFirst = repeatStep.untilFirst;
                    }

                    pathCount++;
                    ReplacedStep replacedStep = ReplacedStep.from(this.sqlgGraph.getSchemaManager(), (AbstractStep) step, pathCount);
                    if (emit) {
                        //the previous step must be marked as emit.
                        //this is because emit() before repeat() indicates that the incoming element for every repeat must be emitted.
                        //i.e. g.V().hasLabel('A').emit().repeat(out('b', 'c')) means A and B must be emitted
                        List<ReplacedStep> previousReplacedSteps = sqlgStep.getReplacedSteps();
                        ReplacedStep previousReplacedStep;
                        if (emitFirst) {
                            previousReplacedStep = previousReplacedSteps.get(previousReplacedSteps.size() - 1);
                            pathCount--;
                        } else {
                            previousReplacedStep = replacedStep;
                        }
                        previousReplacedStep.setEmit(true);
                        previousReplacedStep.setUntilFirst(untilFirst);
                        previousReplacedStep.addLabel((pathCount) + BaseSqlgStrategy.EMIT_LABEL_SUFFIX + BaseSqlgStrategy.SQLG_PATH_FAKE_LABEL);
                        //Remove the path label if there is one. No need for 2 labels as emit labels go onto the path anyhow.
                        previousReplacedStep.getLabels().remove((pathCount) + BaseSqlgStrategy.PATH_LABEL_SUFFIX + BaseSqlgStrategy.SQLG_PATH_FAKE_LABEL);
                        if (emitFirst) {
                            pathCount++;
                        }
                    }
                    if (replacedStep.getLabels().isEmpty()) {
                        boolean precedesPathStep = precedesPathOrTreeStep(steps, stepIterator.nextIndex());
                        if (precedesPathStep) {
                            replacedStep.addLabel(pathCount + BaseSqlgStrategy.PATH_LABEL_SUFFIX + BaseSqlgStrategy.SQLG_PATH_FAKE_LABEL);
                        }
                    }
                    if (previous == null) {
                        sqlgStep = constructSqlgStep(traversal, startStep);
                        sqlgStep.addReplacedStep(replacedStep);
                        handleFirstReplacedStep(startStep, sqlgStep, traversal);
                        collectHasSteps(stepIterator, traversal, replacedStep, pathCount);
                    } else {
                        sqlgStep.addReplacedStep(replacedStep);
                        if (!repeatStepAdded) {
                            //its not in the traversal, so do not remove it
                            traversal.removeStep(step);
                        }
                        collectHasSteps(stepIterator, traversal, replacedStep, pathCount);
                    }
                    previous = step;
                    lastReplacedStep = replacedStep;
                } else {
                    if (doLastEntry(step, stepIterator, traversal, lastReplacedStep, sqlgStep)) {
                        break;
                    }
                    previous = null;
                    lastReplacedStep = null;
                }
            }
        }
    }

}
