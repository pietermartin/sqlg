package org.umlg.sqlg.strategy.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.step.barrier.SqlgUnionStepBarrier;
import org.umlg.sqlg.strategy.SqlgGraphStepStrategy;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgTraversalUtil;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2019/08/21
 */
public class SqlgUnionStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {


    public SqlgUnionStepStrategy() {
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
            Optional<UnionStep> unionStepOptional = TraversalHelper.getLastStepOfAssignableClass(UnionStep.class, traversal);
            if (unionStepOptional.isPresent()) {
                UnionStep<?, ?> unionStep = unionStepOptional.get();

                //Any traversal with a reducing barrier step can not be optimized. As of yet...
                List<? extends Traversal.Admin<?, ?>> globalChildren = unionStep.getGlobalChildren();
                for (Traversal.Admin<?, ?> globalChild : globalChildren) {
                    List<ReducingBarrierStep> reducingBarrierSteps = TraversalHelper.getStepsOfAssignableClassRecursively(ReducingBarrierStep.class, globalChild);
                    if (!reducingBarrierSteps.isEmpty()) {
                        return;
                    }
                }
                //Any traversal with a range can not be optimized. As of yet...
                globalChildren = unionStep.getGlobalChildren();
                for (Traversal.Admin<?, ?> globalChild : globalChildren) {
                    List<RangeGlobalStep> rangeGlobalSteps = TraversalHelper.getStepsOfAssignableClassRecursively(RangeGlobalStep.class, globalChild);
                    if (!rangeGlobalSteps.isEmpty()) {
                        return;
                    }
                }

                //Any traversal with a sample can not be optimized.
                globalChildren = unionStep.getGlobalChildren();
                for (Traversal.Admin<?, ?> globalChild : globalChildren) {
                    List<SampleGlobalStep> sampleGlobalSteps = TraversalHelper.getStepsOfAssignableClassRecursively(SampleGlobalStep.class, globalChild);
                    if (!sampleGlobalSteps.isEmpty()) {
                        return;
                    }
                }

                SqlgUnionStepBarrier<?, ?> sqlgUnionStepBarrier = new SqlgUnionStepBarrier<>(traversal, unionStep);
                for (String label : unionStep.getLabels()) {
                    sqlgUnionStepBarrier.addLabel(label);
                }

                //noinspection unchecked
                TraversalHelper.replaceStep((Step) unionStep, sqlgUnionStepBarrier, traversal);
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
                SqlgGraphStepStrategy.class
        ).collect(Collectors.toSet());
    }

}
