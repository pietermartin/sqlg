package org.umlg.sqlg.strategy;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.SqlgGraphStep;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Date: 2014/07/12
 * Time: 5:45 AM
 */
public class SqlgGraphStepStrategy extends BaseSqlgStrategy {

    private static final List<Class> CONSECUTIVE_STEPS_TO_REPLACE = Arrays.asList(VertexStep.class, EdgeVertexStep.class, GraphStep.class);
    private SqlgGraph sqlgGraph;
    private Logger logger = LoggerFactory.getLogger(SqlgVertexStepStrategy.class.getName());

    public SqlgGraphStepStrategy(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final Step<?, ?> startStep = traversal.getStartStep();
        if (startStep instanceof GraphStep) {
            final GraphStep<?> originalGraphStep = (GraphStep) startStep;
            //Replace all consecutive VertexStep and HasStep with one step
            Step previous = null;
            SqlgGraphStepCompiled sqlgGraphStepCompiled = null;
            List<Step> steps = new ArrayList<>(traversal.asAdmin().getSteps());
            ListIterator<Step> stepIterator = steps.listIterator();
            while (stepIterator.hasNext()) {
                Step step = stepIterator.next();
                //The point of the optimization is to reduce the Paths so the result will be inaccurate as some paths are skipped.
                if (CONSECUTIVE_STEPS_TO_REPLACE.contains(step.getClass())) {
                    if (!mayNotBeOptimized(steps, stepIterator.nextIndex())) {
                        ReplacedStep replacedStep = ReplacedStep.from(this.sqlgGraph.getSchemaManager(), (AbstractStep)step, new ArrayList<>());
                        if (previous == null) {
                            sqlgGraphStepCompiled = new SqlgGraphStepCompiled(this.sqlgGraph, traversal, originalGraphStep.getReturnClass(), originalGraphStep.getIds());
                            TraversalHelper.replaceStep(step, sqlgGraphStepCompiled, traversal);
                            collectHasSteps(stepIterator, traversal, replacedStep);
                            if (originalGraphStep.getIds().length > 0) {
                                //will get optimize via SqlgVertexStepStrategy
                                break;
                            }
                        } else {
                            traversal.removeStep(step);
                            collectHasSteps(stepIterator, traversal, replacedStep);
                        }
                        previous = step;
                        sqlgGraphStepCompiled.addReplacedStep(replacedStep);

                    } else {
                        logger.debug("gremlin not optimized due to path or tree step. " + traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
                    }
                } else {
                    previous = null;
                }
            }
//            final GraphStep<?> originalGraphStep = (GraphStep) startStep;
//            final SqlgGraphStep<?> sqlgGraphStep = new SqlgGraphStep<>(originalGraphStep);
//            traversal.addStep(traversal.getSteps().indexOf(originalGraphStep), sqlgGraphStep);
//            traversal.removeStep(originalGraphStep);
//
//            Step<?, ?> currentStep = sqlgGraphStep.getNextStep();
//            while (true) {
//                if (currentStep instanceof HasContainerHolder) {
//                    sqlgGraphStep.hasContainers.addAll(((HasContainerHolder) currentStep).getHasContainers());
//                    if (!currentStep.getLabels().isEmpty()) {
//                        final IdentityStep identityStep = new IdentityStep<>(traversal);
//                        currentStep.getLabels().forEach(identityStep::addLabel);
//                        TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
//                    }
//                    traversal.removeStep(currentStep);
//                } else if (currentStep instanceof IdentityStep) {
//                    // do nothing
//                } else {
//                    break;
//                }
//                currentStep = currentStep.getNextStep();
//            }
        }
    }

//    @Override
//    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
//        return Stream.of(SqlgVertexStepStrategy.class).collect(Collectors.toSet());
//    }
}
