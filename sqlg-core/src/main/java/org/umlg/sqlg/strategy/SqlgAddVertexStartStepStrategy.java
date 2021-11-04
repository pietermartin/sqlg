package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventCallback;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.step.SqlgAddVertexStartStep;
import org.umlg.sqlg.strategy.barrier.SqlgVertexStepStrategy;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgTraversalUtil;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/08/26
 */
public class SqlgAddVertexStartStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        //Only optimize SqlgGraph. StarGraph also passes through here.
        if (traversal.getGraph().isEmpty() || !(traversal.getGraph().orElseThrow(IllegalStateException::new) instanceof SqlgGraph)) {
            return;
        }
        if (!SqlgTraversalUtil.mayOptimize(traversal)) {
            return;
        }
        List<AddVertexStartStep> addVertexStartSteps = TraversalHelper.getStepsOfAssignableClassRecursively(AddVertexStartStep.class, traversal);
        for (AddVertexStartStep addVertexStartStep : addVertexStartSteps) {
            SqlgAddVertexStartStep sqlgAddVertexStartStep = new SqlgAddVertexStartStep(
                    addVertexStartStep.getTraversal(),
                    addVertexStartStep.getParameters()
            );
            for (EventCallback<Event.VertexAddedEvent> callback : addVertexStartStep.getMutatingCallbackRegistry().getCallbacks()) {
                sqlgAddVertexStartStep.getMutatingCallbackRegistry().addCallback(callback);
            }
            for (String label : addVertexStartStep.getLabels()) {
                sqlgAddVertexStartStep.addLabel(label);
            }
            //noinspection unchecked
            TraversalHelper.replaceStep(
                    addVertexStartStep,
                    sqlgAddVertexStartStep,
                    addVertexStartStep.getTraversal()
            );
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return Stream.of(
                SqlgGraphStepStrategy.class,
                SqlgVertexStepStrategy.class
        ).collect(Collectors.toSet());
    }

//    @Override
//    public Set<Class<? extends DecorationStrategy>> applyPost() {
//        return Stream.of(
////                SqlgGraphStepStrategy.class,
////                SqlgVertexStepStrategy.class,
//                EventStrategy.class
//        ).collect(Collectors.toSet());
//    }
}
