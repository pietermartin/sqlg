package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Compare;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Date: 2014/08/15
 * Time: 7:34 PM
 */
public class SqlgVertexStepStrategy extends AbstractTraversalStrategy {

    private static final List<Class> CONSECUTIVE_STEPS_TO_REPLACE = Arrays.asList(VertexStep.class);
    private static final List<BiPredicate> SUPPORTED_BI_PREDICATE = Arrays.asList(Compare.eq);
    private SqlgGraph sqlgGraph;
    private Logger logger = LoggerFactory.getLogger(SqlgVertexStepStrategy.class.getName());

    public SqlgVertexStepStrategy(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
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
                            if (currentStep.getLabel().isPresent()) {
                                final IdentityStep identityStep = new IdentityStep<>(traversal);
                                identityStep.setLabel(currentStep.getLabel().get());
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
            SqlgVertexStepCompiled sqlgVertexStepCompiled = null;
            List<Step> steps = new ArrayList<>(traversal.asAdmin().getSteps());
            ListIterator<Step> stepIterator = steps.listIterator();
            while (stepIterator.hasNext()) {
                Step step = stepIterator.next();
                //the label check is to ignore any 'as('x')' gremlin for now
                //if there is path() steps then the optimizations can not be used.
                //The point of the optimization is to reduce the Paths so the result will be inaccurate as some paths are skipped.
                if (CONSECUTIVE_STEPS_TO_REPLACE.contains(step.getClass()) && !step.getLabel().isPresent()) {
                    if (!mayNotBeOptimized(steps, stepIterator.nextIndex())) {
                        Pair<Step<?, ?>, List<HasContainer>> stepPair = Pair.of(step, new ArrayList<>());
                        if (previous == null) {
                            sqlgVertexStepCompiled = new SqlgVertexStepCompiled(traversal);
                            TraversalHelper.replaceStep(step, sqlgVertexStepCompiled, traversal);
                            collectHasSteps(stepIterator, traversal, stepPair);
                        } else {
                            traversal.removeStep(step);
                            collectHasSteps(stepIterator, traversal, stepPair);
                        }
                        previous = step;
                        sqlgVertexStepCompiled.addReplacedStep(stepPair);
                    } else {
                        logger.warn("gremlin not optimized due to path or tree step. " + traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
                    }
                } else {
                    previous = null;
                }
            }
        }

    }

    private boolean mayNotBeOptimized(List<Step> steps, int index) {
        List<Step> toCome = steps.subList(index, steps.size());
        return toCome.stream().anyMatch(s -> s.getClass().equals(PathStep.class) || s.getClass().equals(TreeStep.class) || s.getClass().equals(TreeSideEffectStep.class));
    }

    private void collectHasSteps(ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, Pair<Step<?, ?>, List<HasContainer>> stepPair) {
        //Collect the hasSteps
        while (iterator.hasNext()) {
            Step<?, ?> currentStep = iterator.next();
            if (currentStep instanceof HasContainerHolder && ((HasContainerHolder) currentStep).getHasContainers().size() != 1) {
                throw new IllegalStateException("Only handle HasContainerHolder with one HasContainer: BUG");
            }
            if (currentStep instanceof HasContainerHolder && SUPPORTED_BI_PREDICATE.contains(((HasContainerHolder) currentStep).getHasContainers().get(0).predicate)) {
                if (currentStep.getLabel().isPresent()) {
                    final IdentityStep identityStep = new IdentityStep<>(traversal);
                    identityStep.setLabel(currentStep.getLabel().get());
                    TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                }
                iterator.remove();
                traversal.removeStep(currentStep);
                stepPair.getValue().addAll(((HasContainerHolder) currentStep).getHasContainers());
            } else if (currentStep instanceof IdentityStep) {
                // do nothing
            } else {
                iterator.previous();
                break;
            }
        }
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPrior() {
        return Stream.of(PartitionStrategy.class, SubgraphStrategy.class).collect(Collectors.toSet());
    }
}
