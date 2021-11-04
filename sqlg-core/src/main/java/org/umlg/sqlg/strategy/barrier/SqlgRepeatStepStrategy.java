package org.umlg.sqlg.strategy.barrier;

import org.apache.commons.lang3.Range;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.step.barrier.SqlgRepeatStepBarrier;
import org.umlg.sqlg.strategy.SqlgGraphStepStrategy;
import org.umlg.sqlg.strategy.SqlgRangeHolder;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgTraversalUtil;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2014/08/15
 */
public class SqlgRepeatStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {


    public SqlgRepeatStepStrategy() {
        super();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        //Only optimize SqlgGraph. StarGraph also passes through here.
        if (traversal.getGraph().isEmpty() || !(traversal.getGraph().orElseThrow(IllegalStateException::new) instanceof SqlgGraph)) {
            return;
        }
        if (!SqlgTraversalUtil.mayOptimize(traversal)) {
            return;
        }
        while (true) {
            Optional<RepeatStep> repeatStepOptional = TraversalHelper.getLastStepOfAssignableClass(RepeatStep.class, traversal);
            if (repeatStepOptional.isPresent()) {
                RepeatStep<?> repeatStep = repeatStepOptional.get();

                //Any traversal with a reducing barrier step can not be optimized. As of yet...
                List<? extends Traversal.Admin<?, ?>> localChildren = repeatStep.getLocalChildren();
                for (Traversal.Admin<?, ?> localChild : localChildren) {
                    List<ReducingBarrierStep> reducingBarrierSteps = TraversalHelper.getStepsOfAssignableClassRecursively(ReducingBarrierStep.class, localChild);
                    if (!reducingBarrierSteps.isEmpty()) {
                        return;
                    }
                }
                //Any traversal with a range can not be optimized. As of yet...
                localChildren = repeatStep.getLocalChildren();
                for (Traversal.Admin<?, ?> localChild : localChildren) {
                    List<RangeGlobalStep> rangeGlobalSteps = TraversalHelper.getStepsOfAssignableClassRecursively(RangeGlobalStep.class, localChild);
                    if (!rangeGlobalSteps.isEmpty()) {
                        return;
                    }
                }

                //Any traversal with a sample can not be optimized.
                localChildren = repeatStep.getLocalChildren();
                for (Traversal.Admin<?, ?> localChild : localChildren) {
                    List<SampleGlobalStep> sampleGlobalSteps = TraversalHelper.getStepsOfAssignableClassRecursively(SampleGlobalStep.class, localChild);
                    if (!sampleGlobalSteps.isEmpty()) {
                        return;
                    }
                }

                SqlgRepeatStepBarrier<?> sqlgRepeatStepBarrier = new SqlgRepeatStepBarrier<>(traversal, repeatStep);
                for (String label : repeatStep.getLabels()) {
                    sqlgRepeatStepBarrier.addLabel(label);
                }

                int indexOfRepeatStep = traversal.getSteps().indexOf(repeatStep);
                if ((traversal.getSteps().size() > indexOfRepeatStep + 1) && traversal.getSteps().get(indexOfRepeatStep + 1) instanceof RangeGlobalStep) {
                    RangeGlobalStep<?> rgs = (RangeGlobalStep<?>) traversal.getSteps().get(indexOfRepeatStep + 1);
                    long high = rgs.getHighRange();
                    if (high == -1) {
                        //skip step
                        sqlgRepeatStepBarrier.setSqlgRangeHolder(SqlgRangeHolder.from(rgs.getLowRange()));
                    } else {
                        sqlgRepeatStepBarrier.setSqlgRangeHolder(SqlgRangeHolder.from(Range.between(rgs.getLowRange(), high)));
                    }
                    for (String label : rgs.getLabels()) {
                        sqlgRepeatStepBarrier.addLabel(label);
                    }
                    traversal.removeStep(indexOfRepeatStep + 1);
                }
                //noinspection unchecked
                TraversalHelper.replaceStep((Step) repeatStep, sqlgRepeatStepBarrier, traversal);
            } else {
                break;
            }
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPost() {
        return Stream.of(
                SqlgVertexStepStrategy.class
        ).collect(Collectors.toSet());
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return Stream.of(
                SqlgGraphStepStrategy.class,
                CountStrategy.class
        ).collect(Collectors.toSet());
    }

}
