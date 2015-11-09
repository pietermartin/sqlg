package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.structure.SqlgGraph;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Date: 2014/08/15
 * Time: 7:34 PM
 */
//TODO optimize Order step as that is used in UMLG for Sequences
public class SqlgVertexStepStrategy extends BaseSqlgStrategy {

    private static final List<Class> CONSECUTIVE_STEPS_TO_REPLACE = Arrays.asList(VertexStep.class, EdgeVertexStep.class);
    private Logger logger = LoggerFactory.getLogger(SqlgVertexStepStrategy.class.getName());

    public SqlgVertexStepStrategy(SqlgGraph sqlgGraph) {
        super(sqlgGraph);
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        //This is because in normal BatchMode the new vertices are cached with it edges.
        //The query will read from the cache if this is for a cached vertex
        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchModeNormal()) {
            List<VertexStep> vertexSteps = TraversalHelper.getStepsOfClass(VertexStep.class, traversal);

            vertexSteps.forEach(
                    (s) -> {
                        SqlgVertexStep<Vertex> insertStep = new SqlgVertexStep<Vertex>(s.getTraversal(), s.getReturnClass(), s.getDirection(), s.getEdgeLabels());
                        TraversalHelper.replaceStep(
                                s,
                                insertStep,
                                traversal);
                    }
            );

            //The HasSteps following directly after a VertexStep is merged into the SqlgVertexStep
            Set<Step> toRemove = new HashSet<>();
            for (Step<?, ?> step : traversal.asAdmin().getSteps()) {
                if (step instanceof SqlgVertexStep && Vertex.class.isAssignableFrom(((SqlgVertexStep) step).getReturnClass())) {
                    SqlgVertexStep sqlgVertexStep = (SqlgVertexStep) step;
                    Step<?, ?> currentStep = sqlgVertexStep.getNextStep();
                    while (true) {
                        if (currentStep instanceof HasContainerHolder) {
                            sqlgVertexStep.hasContainers.addAll(((HasContainerHolder) currentStep).getHasContainers());
                            if (!currentStep.getLabels().isEmpty()) {
                                final IdentityStep identityStep = new IdentityStep<>(traversal);
                                currentStep.getLabels().forEach(identityStep::addLabel);
                                TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                            }
                            toRemove.add(currentStep);
                        } else if (currentStep instanceof IdentityStep) {
                            // do nothing
                        } else {
                            break;
                        }
                        currentStep = currentStep.getNextStep();
                    }
                }
            }
            toRemove.forEach(traversal::removeStep);
        } else {
            Class repeatStepClass;
            Class loopTraversalClass;
            try {
                repeatStepClass = Class.forName("org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep");
                loopTraversalClass = Class.forName("org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            //Replace all consecutive VertexStep and HasStep with one step
            int pathCount = 0;
            boolean repeatStepAdded = false;
            int repeatStepsAdded = 0;
            Step previous = null;
            ReplacedStep<?, ?> lastReplacedStep = null;
            SqlgVertexStepCompiled sqlgVertexStepCompiled = null;
            List<Step> steps = new ArrayList<>(traversal.asAdmin().getSteps());
            ListIterator<Step> stepIterator = steps.listIterator();
            while (stepIterator.hasNext()) {
                if (this.canNotBeOptimized(steps, stepIterator.nextIndex())) {
                    logger.debug("gremlin not optimized due to path or tree step. " + traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
                    return;
                }
                if (unoptimizableRepeat(steps, stepIterator.nextIndex())) {
                    logger.debug("gremlin not optimized due to RepeatStep with emit. " + traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
                    return;
                }
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
                    //The point of the optimization is to reduce the Paths so the result will be inaccurate as some paths are skipped.
                    if (CONSECUTIVE_STEPS_TO_REPLACE.contains(step.getClass())) {
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
                            List<ReplacedStep> previousReplacedSteps = sqlgVertexStepCompiled.getReplacedSteps();
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
                        }
                        if (replacedStep.getLabels().isEmpty()) {
                            boolean precedesPathStep = precedesPathOrTreeStep(steps, stepIterator.nextIndex());
                            if (precedesPathStep) {
                                replacedStep.addLabel(pathCount + BaseSqlgStrategy.PATH_LABEL_SUFFIX + BaseSqlgStrategy.SQLG_PATH_FAKE_LABEL);
                            }
                        }
                        pathCount++;
                        if (replacedStep.getLabels().isEmpty()) {
                            //if the step is before a PathStep and is not labeled, add a fake label in order for the sql to return its values.
                            boolean precedesPathStep = precedesPathOrTreeStep(steps, stepIterator.nextIndex());
                            if (precedesPathStep) {
                                replacedStep.addLabel(pathCount + BaseSqlgStrategy.PATH_LABEL_SUFFIX + BaseSqlgStrategy.SQLG_PATH_FAKE_LABEL);
                            }
                        }
                        if (previous == null) {
                            sqlgVertexStepCompiled = new SqlgVertexStepCompiled(traversal);
                            sqlgVertexStepCompiled.addReplacedStep(replacedStep);
                            TraversalHelper.replaceStep(step, sqlgVertexStepCompiled, traversal);
                            collectHasSteps(stepIterator, traversal, replacedStep, pathCount);
                        } else {
                            sqlgVertexStepCompiled.addReplacedStep(replacedStep);
                            if (!repeatStepAdded) {
                                //its not in the traversal, so do not remove it
                                traversal.removeStep(step);
                            }
                            collectHasSteps(stepIterator, traversal, replacedStep, pathCount);
                        }
                        previous = step;
                        lastReplacedStep = replacedStep;
                        sqlgVertexStepCompiled.addReplacedStep(replacedStep);
                    } else {
                        if (lastReplacedStep != null) {
                            replaceOrderGlobalSteps(step, stepIterator, traversal, lastReplacedStep);
                        }
                        previous = null;
                        lastReplacedStep = null;
                    }
                }
            }
        }

    }

    private static void replaceOrderGlobalSteps(Step step, ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> replacedStep) {
        //Collect the OrderGlobalSteps
        if (step instanceof OrderGlobalStep && isElementValueComparator((OrderGlobalStep) step)) {
            TraversalHelper.replaceStep(step, new SqlgOrderGlobalStep<>((OrderGlobalStep) step), traversal);
            iterator.remove();
//            traversal.removeStep(step);
            replacedStep.getComparators().addAll(((OrderGlobalStep) step).getComparators());
        } else {
            replaceSelectOrderGlobalSteps(iterator, traversal, replacedStep);
        }
    }

    private static void replaceSelectOrderGlobalSteps(ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> replacedStep) {
        //Collect the OrderGlobalSteps
        while (iterator.hasNext()) {
            Step currentStep = iterator.next();
            if (currentStep instanceof OrderGlobalStep && isElementValueComparator((OrderGlobalStep) currentStep)) {
                iterator.remove();
                TraversalHelper.replaceStep(currentStep, new SqlgOrderGlobalStep<>((OrderGlobalStep) currentStep), traversal);
//                traversal.removeStep(currentStep);
                replacedStep.getComparators().addAll(((OrderGlobalStep) currentStep).getComparators());
            } else if (currentStep instanceof OrderGlobalStep && isTraversalComparatorWithSelectOneStep((OrderGlobalStep) currentStep)) {
                iterator.remove();
                TraversalHelper.replaceStep(currentStep, new SqlgOrderGlobalStep<>((OrderGlobalStep) currentStep), traversal);
//                traversal.removeStep(currentStep);
                replacedStep.getComparators().addAll(((OrderGlobalStep) currentStep).getComparators());
            } else if (currentStep instanceof IdentityStep) {
                // do nothing
            } else {
                iterator.previous();
                break;
            }
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return Stream.of(SqlgGraphStepStrategy.class).collect(Collectors.toSet());
    }
}
