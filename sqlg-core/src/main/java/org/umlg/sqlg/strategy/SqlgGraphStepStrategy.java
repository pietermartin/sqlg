package org.umlg.sqlg.strategy;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
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
        int countStepsToGoBack = 0;
        while (stepIterator.hasNext()) {
            Step step = stepIterator.next();

            if (step instanceof RepeatStep) {
                countStepsToGoBack = 0;
                repeatStepAdded = false;
                RepeatStep repeatStep = (RepeatStep) step;
                List<Traversal.Admin> repeatTraversals = repeatStep.<Traversal.Admin>getGlobalChildren();
                Traversal.Admin admin = repeatTraversals.get(0);
                List<Step> internalRepeatSteps = admin.getSteps();
                //There must be a until traversal and no emit traversal
                LoopTraversal untilTraversal = (LoopTraversal) repeatStep.<LoopTraversal>getLocalChildren().stream().filter(c->c instanceof LoopTraversal).findAny().get();
                long numberOfLoops = untilTraversal.getMaxLoops();
                for (int i = 0; i < numberOfLoops; i++) {
                    for (Step internalRepeatStep : internalRepeatSteps) {
                        if (internalRepeatStep instanceof RepeatStep.RepeatEndStep) {
                            break;
                        }
                        pathCount++;
                        stepIterator.add(internalRepeatStep);
                        stepIterator.previous();
                        stepIterator.next();
                        repeatStepAdded = true;
                        countStepsToGoBack++;
                    }
                }
                traversal.removeStep(repeatStep);
                //this is needed to the next() to be the added step
                for (int i = 0; i < countStepsToGoBack; i++) {
                    stepIterator.previous();
                }
            } else {

                if (CONSECUTIVE_STEPS_TO_REPLACE.contains(step.getClass())) {

                    //check if the step is a repeat step
                    boolean emit = false;
                    if (countStepsToGoBack > 0) {
                        countStepsToGoBack--;
                        RepeatStep repeatStep = (RepeatStep) step.getTraversal().getParent();
                        emit =  repeatStep.getEmitTraversal() != null;
                    }

                    pathCount++;
                    ReplacedStep replacedStep = ReplacedStep.from(this.sqlgGraph.getSchemaManager(), (AbstractStep) step, pathCount);
                    if (emit) {
                        //for now pretend is a emit first
                        //the previous step must be marked as emit
                        List<ReplacedStep> previousReplacedSteps = sqlgGraphStepCompiled.getReplacedSteps();
                        ReplacedStep previousReplacedStep = previousReplacedSteps.get(previousReplacedSteps.size() - 1);
                        previousReplacedStep.setEmit(true);
                        if (previousReplacedStep.getLabels().isEmpty()) {
                            previousReplacedStep.addLabel(pathCount + BaseSqlgStrategy.PATH_LABEL_SUFFIX + BaseSqlgStrategy.SQLG_PATH_FAKE_LABEL);
                        }
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

