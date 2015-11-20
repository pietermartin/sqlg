package org.umlg.sqlg.strategy;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.*;
import java.util.stream.Stream;

/**
 * Date: 2014/07/12
 * Time: 5:45 AM
 */
public class SqlgGraphStepStrategy extends BaseSqlgStrategy {

    private static final List<Class> CONSECUTIVE_STEPS_TO_REPLACE = Arrays.asList(VertexStep.class, EdgeVertexStep.class, GraphStep.class, EdgeOtherVertexStep.class);
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

        if (originalGraphStep.getIds().length > 0 && this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchModeNormal()) {

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
            if (this.mayNotBeOptimized(steps, stepIterator.nextIndex())) {
                logger.debug("gremlin not optimized due to path or tree step. " + traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
                return;
            }
            if (originalGraphStep.getIds().length>0) {
                Class clazz = originalGraphStep.getIds()[0].getClass();
                if (!Stream.of(originalGraphStep.getIds()).allMatch(id -> clazz.isAssignableFrom(id.getClass())))
                    throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
            }

            //Replace all consecutive VertexStep and HasStep with one step
            SqlgGraphStepCompiled sqlgGraphStepCompiled = null;
            Step previous = null;
            ReplacedStep<?, ?> lastReplacedStep = null;

            int pathCount = 0;
            while (stepIterator.hasNext()) {
                Step step = stepIterator.next();
                if (CONSECUTIVE_STEPS_TO_REPLACE.contains(step.getClass())) {
                    pathCount++;
                    ReplacedStep replacedStep = ReplacedStep.from(this.sqlgGraph.getSchemaManager(), (AbstractStep) step, pathCount);
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
                        traversal.removeStep(step);
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

//                } else if (step instanceof RepeatStep) {
//                    RepeatStep repeatStep = (RepeatStep) step;
//                    List<Traversal.Admin> repeatTraversals = repeatStep.<Traversal.Admin>getGlobalChildren();
//                    Traversal.Admin admin = repeatTraversals.get(0);
//                    List<Step> repeatVertexSteps = admin.getSteps();
//                    VertexStep vertexStep = (VertexStep) repeatVertexSteps.get(0);
//
//                    List<LoopTraversal> untilTraversals = repeatStep.<LoopTraversal>getLocalChildren();
//                    LoopTraversal untilTraversal = untilTraversals.get(0);
//                    long numberOfLoops = untilTraversal.getMaxLoops();
//                    for (int i = 1; i <= numberOfLoops; i++) {
//                        ReplacedStep replacedStep = ReplacedStep.from(this.sqlgGraph.getSchemaManager(), vertexStep);
////                        replacedStep.emit();
////                        replacedStep.path();
//                        replacedStep.addLabel("a" + i);
//                        sqlgGraphStepCompiled.addReplacedStep(replacedStep);
//                    }
//                    traversal.removeStep(repeatStep);
