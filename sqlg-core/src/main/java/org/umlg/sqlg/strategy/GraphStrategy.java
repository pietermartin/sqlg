package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/03/04
 *         <p>
 *         Got tired of TinkerPop's static strategy vibe.
 *         Thank the good Lord for OO.
 */
public class GraphStrategy extends BaseStrategy {

    private Logger logger = LoggerFactory.getLogger(GraphStrategy.class.getName());
    private static final List<Class> CONSECUTIVE_STEPS_TO_REPLACE = Arrays.asList(
            VertexStep.class, EdgeVertexStep.class, GraphStep.class, EdgeOtherVertexStep.class
            , OrderGlobalStep.class, RangeGlobalStep.class
            , ChooseStep.class
            , RepeatStep.class

    );

    private GraphStrategy(Traversal.Admin<?, ?> traversal) {
        super(traversal);
    }

    public static GraphStrategy from(Traversal.Admin<?, ?> traversal) {
        return new GraphStrategy(traversal);
    }

    void apply() {
        final Step<?, ?> startStep = traversal.getStartStep();

        //Only optimize graph step.
//        if (!(startStep instanceof GraphStep) || !(this.graph instanceof SqlgGraph)) {
//            return;
//        }
        if (!(startStep instanceof GraphStep)) {
            return;
        }
        final GraphStep originalGraphStep = (GraphStep) startStep;

        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInNormalBatchMode()) {
            this.sqlgGraph.tx().flush();
        }

        if (originalGraphStep.getIds().length > 0) {
            Object id = originalGraphStep.getIds()[0];
            if (id != null) {
                Class clazz = id.getClass();
                if (!Stream.of(originalGraphStep.getIds()).allMatch(i -> clazz.isAssignableFrom(i.getClass())))
                    throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
            }
        }
        if (this.canNotBeOptimized()) {
            this.logger.debug("gremlin not optimized due to path or tree step. " + this.traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
            return;
        }
        combineSteps();

    }

    @Override
    protected SqlgStep constructSqlgStep(Step startStep) {
        Preconditions.checkArgument(startStep instanceof GraphStep, "Expected a GraphStep, found instead a " + startStep.getClass().getName());
        GraphStep<?, ?> graphStep = (GraphStep) startStep;
        return new SqlgGraphStepCompiled(this.sqlgGraph, this.traversal, graphStep.getReturnClass(), graphStep.isStartStep(), graphStep.getIds());
    }

    @Override
    protected boolean isReplaceableStep(Class<? extends Step> stepClass, boolean alreadyReplacedGraphStep) {
        return CONSECUTIVE_STEPS_TO_REPLACE.contains(stepClass);// && !(stepClass.isAssignableFrom(GraphStep.class) && alreadyReplacedGraphStep);
    }

    @Override
    protected void replaceStepInTraversal(Step stepToReplace, SqlgStep sqlgStep) {
        TraversalHelper.replaceStep(stepToReplace, sqlgStep, this.traversal);
    }
}
