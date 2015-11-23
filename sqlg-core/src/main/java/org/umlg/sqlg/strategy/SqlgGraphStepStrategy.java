package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Stream;

/**
 * Date: 2014/07/12
 * Time: 5:45 AM
 */
public class SqlgGraphStepStrategy extends BaseSqlgStrategy {

    private static final List<Class> CONSECUTIVE_STEPS_TO_REPLACE = Arrays.asList(VertexStep.class, EdgeVertexStep.class, GraphStep.class, EdgeOtherVertexStep.class);
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

        if (originalGraphStep.getIds().length > 0 && this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchModeNormal()) {
            readFromCache(traversal);
        } else {
            if (originalGraphStep.getIds().length > 0) {
                Class clazz = originalGraphStep.getIds()[0].getClass();
                if (!Stream.of(originalGraphStep.getIds()).allMatch(id -> clazz.isAssignableFrom(id.getClass())))
                    throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
            }
            final List<Step> steps = new ArrayList<>(traversal.asAdmin().getSteps());
            final ListIterator<Step> stepIterator = steps.listIterator();
            if (this.canNotBeOptimized(steps, stepIterator.nextIndex())) {
                this.logger.debug("gremlin not optimized due to path or tree step. " + traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
                return;
            }
            combineSteps(traversal, steps, stepIterator);
        }
    }


    @Override
    protected SqlgStep constructSqlgStep(Traversal.Admin<?, ?> traversal, Step startStep) {
        Preconditions.checkArgument(startStep instanceof GraphStep, "Expected a GraphStep, found instead a " + startStep.getClass().getName());
        return new SqlgGraphStepCompiled(this.sqlgGraph, traversal, ((GraphStep)startStep).getReturnClass(), ((GraphStep)startStep).getIds());
    }

    @Override
    protected boolean isReplaceableStep(Class<? extends Step> stepClass) {
        return CONSECUTIVE_STEPS_TO_REPLACE.contains(stepClass);
    }

    @Override
    protected void handleFirstReplacedStep(Step firstStep, SqlgStep sqlgStep, Traversal.Admin<?, ?>  traversal) {
        TraversalHelper.replaceStep(firstStep, sqlgStep, traversal);
    }

    @Override
    protected boolean doLastEntry(Step step, ListIterator<Step> stepIterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> lastReplacedStep, SqlgStep sqlgStep) {
        if (lastReplacedStep != null) {
            //TODO optimize this, to not parse if there are no OrderGlobalSteps
            sqlgStep.parseForStrategy();
            if (!sqlgStep.isForMultipleQueries()) {
                collectOrderGlobalSteps(step, stepIterator, traversal, lastReplacedStep);
            }
        }
        return true;
    }
}

