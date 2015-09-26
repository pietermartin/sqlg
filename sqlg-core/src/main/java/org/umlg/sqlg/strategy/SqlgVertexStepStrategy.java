package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
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
            //Replace all consecutive VertexStep and HasStep with one step
            Step previous = null;
            ReplacedStep<?, ?> lastReplacedStep = null;
            SqlgVertexStepCompiled sqlgVertexStepCompiled = null;
            List<Step> steps = new ArrayList<>(traversal.asAdmin().getSteps());
            ListIterator<Step> stepIterator = steps.listIterator();
            while (stepIterator.hasNext()) {
                Step step = stepIterator.next();
                //The point of the optimization is to reduce the Paths so the result will be inaccurate as some paths are skipped.
                if (CONSECUTIVE_STEPS_TO_REPLACE.contains(step.getClass())) {
                    if (!mayNotBeOptimized(steps, stepIterator.nextIndex())) {
                        ReplacedStep replacedStep = ReplacedStep.from(this.sqlgGraph.getSchemaManager(), (AbstractStep<?,?>) step);
                        if (previous == null) {
                            sqlgVertexStepCompiled = new SqlgVertexStepCompiled(traversal);
                            TraversalHelper.replaceStep(step, sqlgVertexStepCompiled, traversal);
                            collectHasSteps(stepIterator, traversal, replacedStep);
                        } else {
                            traversal.removeStep(step);
                            collectHasSteps(stepIterator, traversal, replacedStep);
                        }
                        previous = step;
                        lastReplacedStep = replacedStep;
                        sqlgVertexStepCompiled.addReplacedStep(replacedStep);
                    } else {
                        logger.debug("gremlin not optimized due to path or tree step. " + traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
                    }
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
