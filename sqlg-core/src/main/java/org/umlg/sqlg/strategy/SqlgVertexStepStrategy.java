package org.umlg.sqlg.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.traversal.step.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.traversal.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.lang3.tuple.Pair;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.*;
import java.util.function.BiPredicate;

/**
 * Date: 2014/08/15
 * Time: 7:34 PM
 */
public class SqlgVertexStepStrategy extends AbstractTraversalStrategy {

    private static final List<Class> CONSECUTIVE_STEPS_TO_REPLACE = Arrays.asList(VertexStep.class);
    private static final List<BiPredicate> SUPPORTED_BI_PREDICATE = Arrays.asList(Compare.eq);
    private SqlgGraph sqlgGraph;

    public SqlgVertexStepStrategy(SqlgGraph sqlgGraph) {
       this.sqlgGraph = sqlgGraph;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine traversalEngine) {
        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            List<VertexStep> vertexSteps = TraversalHelper.getStepsOfClass(VertexStep.class, traversal);
            vertexSteps.forEach(
                    (s) -> TraversalHelper.replaceStep(s, new SqlgVertexStep(s.getTraversal(), s.getReturnClass(), s.getDirection(), s.getEdgeLabels()), traversal)
            );
            //The HasSteps following directly after a VertexStep is merged into the SqlgVertexStep
            Set<Step> toRemove = new HashSet<>();
            for (Step<?, ?> step : traversal.asAdmin().getSteps()) {
                if (step instanceof SqlgVertexStep && Vertex.class.isAssignableFrom(((SqlgVertexStep) step).returnClass)) {
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
            SqlgVertexStepCompiler sqlgVertexStepCompiler = null;
            List<Step> steps = new ArrayList<>(traversal.asAdmin().getSteps());
            ListIterator<Step> stepIterator = steps.listIterator();
            while (stepIterator.hasNext()) {
                Step step = stepIterator.next();
                if (CONSECUTIVE_STEPS_TO_REPLACE.contains(step.getClass())) {
                    Pair<Step<?, ?>, List<HasContainer>> stepPair = Pair.of(step, new ArrayList<>());
                    if (previous == null) {
                        sqlgVertexStepCompiler = new SqlgVertexStepCompiler(traversal);
                        TraversalHelper.replaceStep(step, sqlgVertexStepCompiler, traversal);
                        collectHasSteps(stepIterator, traversal, stepPair);
                    } else {
                        traversal.removeStep(step);
                        collectHasSteps(stepIterator, traversal, stepPair);
                    }
                    previous = step;
                    sqlgVertexStepCompiler.addReplacedStep(stepPair);
                } else {
                    previous = null;
                }
            }
        }

    }

    private void collectHasSteps(ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, Pair<Step<?, ?>, List<HasContainer>> stepPair) {
        //Collect the hasSteps
        while (iterator.hasNext()) {
            Step<?, ?> currentStep = iterator.next();
            if (currentStep instanceof HasContainerHolder && ((HasContainerHolder)currentStep).getHasContainers().size() != 1) {
                throw new IllegalStateException("Only handle HasContainderHolder with one HasContainer: BUG");
            }
            if (currentStep instanceof HasContainerHolder && SUPPORTED_BI_PREDICATE.contains(((HasContainerHolder)currentStep).getHasContainers().get(0).predicate)) {
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

}
