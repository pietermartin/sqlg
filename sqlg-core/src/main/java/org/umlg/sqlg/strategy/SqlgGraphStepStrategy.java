package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * Date: 2014/07/12
 * Time: 5:45 AM
 */
public class SqlgGraphStepStrategy extends BaseSqlgStrategy {

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

        if (originalGraphStep.getIds().length > 0) {
            return;
        }
        if (this.canNotBeOptimized(steps, stepIterator.nextIndex())) {
            logger.debug("gremlin not optimized due to path or tree step. " + traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
            return;
        }
        if (unoptimizableRepeat(steps, stepIterator.nextIndex())) {
            logger.debug("gremlin not optimized due to RepeatStep with emit. " + traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
            return;
        }
        babySitSteps(traversal, originalGraphStep, steps, stepIterator);
    }

    @Override
    protected SqlgStep constructSqlgStep(Traversal.Admin<?, ?> traversal, Step startStep) {
        Preconditions.checkArgument(startStep instanceof GraphStep, "Expected a GraphStep, found instead a " + startStep.getClass().getName());
        return new SqlgGraphStepCompiled(this.sqlgGraph, traversal, ((GraphStep)startStep).getReturnClass(), ((GraphStep)startStep).getIds());
    }

}

