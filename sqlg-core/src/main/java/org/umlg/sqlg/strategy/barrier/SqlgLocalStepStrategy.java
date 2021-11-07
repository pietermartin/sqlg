package org.umlg.sqlg.strategy.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.step.barrier.SqlgLocalStepBarrier;
import org.umlg.sqlg.strategy.SqlgGraphStepStrategy;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgTraversalUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2014/08/15
 */
public class SqlgLocalStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {


    public SqlgLocalStepStrategy() {
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
            Optional<LocalStep> localStepOptional = TraversalHelper.getLastStepOfAssignableClass(LocalStep.class, traversal);
            if (localStepOptional.isPresent()) {
                LocalStep<?, ?> localStep = localStepOptional.get();

                //Traversal with the following reducing barrier step can not be optimized. As of yet...
                for (Class<? extends ReducingBarrierStep> aClass : Arrays.asList(FoldStep.class, GroupCountStep.class, CountGlobalStep.class, TreeStep.class)) {
                    List<? extends Traversal.Admin<?, ?>> localChildren = localStep.getLocalChildren();
                    for (Traversal.Admin<?, ?> localChild : localChildren) {
                        List<? extends ReducingBarrierStep> foldSteps = TraversalHelper.getStepsOfAssignableClassRecursively(aClass, localChild);
                        if (!foldSteps.isEmpty()) {
                            return;
                        }
                    }

                }
                //Any traversal with a range can not be optimized. As of yet...
                List<? extends Traversal.Admin<?, ?>> localChildren = localStep.getLocalChildren();
                for (Traversal.Admin<?, ?> localChild : localChildren) {
                    List<RangeGlobalStep> rangeGlobalSteps = TraversalHelper.getStepsOfAssignableClassRecursively(RangeGlobalStep.class, localChild);
                    if (!rangeGlobalSteps.isEmpty()) {
                        return;
                    }
                }

                //Any traversal with a sample can not be optimized.
                localChildren = localStep.getLocalChildren();
                for (Traversal.Admin<?, ?> localChild : localChildren) {
                    List<SampleGlobalStep> sampleGlobalSteps = TraversalHelper.getStepsOfAssignableClassRecursively(SampleGlobalStep.class, localChild);
                    if (!sampleGlobalSteps.isEmpty()) {
                        return;
                    }
                }

                SqlgLocalStepBarrier<?, ?> sqlgLocalStepBarrier = new SqlgLocalStepBarrier<>(traversal, localStep);
                for (String label : localStep.getLabels()) {
                    sqlgLocalStepBarrier.addLabel(label);
                }
                //noinspection unchecked
                TraversalHelper.replaceStep((Step) localStep, sqlgLocalStepBarrier, traversal);
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

