package org.umlg.sqlg.strategy;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * Date: 2014/07/12
 * Time: 5:45 AM
 */
public class SqlgGraphStepStrategy extends BaseSqlgStrategy {

    private Logger logger = LoggerFactory.getLogger(SqlgVertexStepStrategy.class.getName());

    public SqlgGraphStepStrategy(SqlgGraph sqlgGraph) {
        super(sqlgGraph);
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final Step<?, ?> startStep = traversal.getStartStep();

        //Only optimize graph step.
        if (!(startStep instanceof GraphStep)) {
            return;
        }

        final GraphStep<?> originalGraphStep = (GraphStep) startStep;
        final List<Step> steps = new ArrayList<>(traversal.asAdmin().getSteps());
        final ListIterator<Step> stepIterator = steps.listIterator();

        if (originalGraphStep.getIds().length > 0) {
            return;
        }
        if (this.canNotBeOptimized(steps, stepIterator.nextIndex())) {
            logger.debug("gremlin not optimized due to path or tree step. " + traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
            return;
        }
        if (unoptimizableRepeat(steps, stepIterator.nextIndex())) {
            logger.debug("gremlin not optimized due to RepeatStep with emit. " + traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
            return;
        }

        //Replace all consecutive VertexStep and HasStep with one step
        SqlgGraphStepCompiled sqlgGraphStepCompiled = null;
        Step previous = null;
        ReplacedStep<?, ?> lastReplacedStep = null;

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
                List<Traversal.Admin> repeatTraversals = repeatStep.<Traversal.Admin>getGlobalChildren();
                Traversal.Admin admin = repeatTraversals.get(0);
                List<Step> internalRepeatSteps = admin.getSteps();
                //this is guaranteed by the previous check unoptimizableRepeat(...)
                LoopTraversal loopTraversal = (LoopTraversal) repeatStep.getUntilTraversal();
                long numberOfLoops = loopTraversal.getMaxLoops();
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

                if (CONSECUTIVE_STEPS_TO_REPLACE.contains(step.getClass())) {

                    //check if repeat steps were added to the stepIterator
                    boolean emit = false;
                    boolean emitFirst = false;
                    if (repeatStepsAdded > 0) {
                        repeatStepsAdded--;
                        RepeatStep repeatStep = (RepeatStep) step.getTraversal().getParent();
                        emit = repeatStep.getEmitTraversal() != null;
                        emitFirst = repeatStep.emitFirst;
                    }

                    pathCount++;
                    ReplacedStep replacedStep = ReplacedStep.from(this.sqlgGraph.getSchemaManager(), (AbstractStep) step, pathCount);
                    if (emit) {
                        //for now pretend is a emit first
                        //the previous step must be marked as emit.
                        //this is because emit() before repeat() indicates that the incoming element for every repeat must be emitted.
                        //i.e. g.V().hasLabel('A').emit().repeat(out('b', 'c')) means A and B must be emitted
                        //
                        List<ReplacedStep> previousReplacedSteps = sqlgGraphStepCompiled.getReplacedSteps();
                        ReplacedStep previousReplacedStep;
                        if (emitFirst) {
                            previousReplacedStep = previousReplacedSteps.get(previousReplacedSteps.size() - 1);
                        } else {
                            previousReplacedStep = replacedStep;
                        }
                        previousReplacedStep.setEmit(true);
                        previousReplacedStep.addLabel((pathCount - 1) + BaseSqlgStrategy.EMIT_LABEL_SUFFIX + BaseSqlgStrategy.SQLG_PATH_FAKE_LABEL);
                        //Remove the path label if there is one. No need for 2 labels as emit labels go onto the path anyhow.
                        previousReplacedStep.getLabels().remove((pathCount - 1) + BaseSqlgStrategy.PATH_LABEL_SUFFIX + BaseSqlgStrategy.SQLG_PATH_FAKE_LABEL);
                    }
                    if (replacedStep.getLabels().isEmpty()) {
                        boolean precedesPathStep = precedesPathOrTreeStep(steps, stepIterator.nextIndex());
                        if (precedesPathStep) {
                            replacedStep.addLabel(pathCount + BaseSqlgStrategy.PATH_LABEL_SUFFIX + BaseSqlgStrategy.SQLG_PATH_FAKE_LABEL);
                        }
                    }
                    if (previous == null) {
                        sqlgGraphStepCompiled = new SqlgGraphStepCompiled(this.sqlgGraph, traversal, originalGraphStep.getReturnClass(), originalGraphStep.getIds());
                        sqlgGraphStepCompiled.addReplacedStep(replacedStep);
                        TraversalHelper.replaceStep(step, sqlgGraphStepCompiled, traversal);
                        collectHasSteps(stepIterator, traversal, replacedStep, pathCount);
                    } else {
                        sqlgGraphStepCompiled.addReplacedStep(replacedStep);
                        if (!repeatStepAdded) {
                            //its not in the traversal, so do not remove it
                            traversal.removeStep(step);
                        }
                        collectHasSteps(stepIterator, traversal, replacedStep, pathCount);
                    }
                    previous = step;
                    lastReplacedStep = replacedStep;
                } else {
                    if (lastReplacedStep != null) {
                        //TODO optimize this, to not parse if there are no OrderGlobalSteps
                        sqlgGraphStepCompiled.parseForStrategy();
                        if (!sqlgGraphStepCompiled.isForMultipleQueries()) {
                            collectOrderGlobalSteps(step, stepIterator, traversal, lastReplacedStep);
                        }
                    }
                    break;
                }
            }
        }
    }


}

