package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.step.SqlgStep;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/03/04
 */
@SuppressWarnings("rawtypes")
public class GraphStrategy extends BaseStrategy {

    private static final Logger logger = LoggerFactory.getLogger(GraphStrategy.class);

    private GraphStrategy(Traversal.Admin<?, ?> traversal) {
        super(traversal);
    }

    public static GraphStrategy from(Traversal.Admin<?, ?> traversal) {
        return new GraphStrategy(traversal);
    }

    void apply() {
        final Step<?, ?> startStep = traversal.getStartStep();

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
                //noinspection unchecked
                if (!Stream.of(originalGraphStep.getIds()).allMatch(i -> clazz.isAssignableFrom(i.getClass())))
                    throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
            }
        }
        if (this.canNotBeOptimized()) {
            logger.debug("gremlin not optimized due to path or tree step. " + this.traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
            return;
        }
        combineSteps();
    }

    void combineSteps() {
        @SuppressWarnings("unchecked")
        List<Step<?, ?>> steps = new ArrayList(this.traversal.asAdmin().getSteps());
        ListIterator<Step<?, ?>> stepIterator = steps.listIterator();
        MutableInt pathCount = new MutableInt(0);
        while (stepIterator.hasNext()) {
            Step<?, ?> step = stepIterator.next();
            if (isReplaceableStep(step.getClass())) {
                stepIterator.previous();
                boolean keepGoing = handleStep(stepIterator, pathCount);
                if (!keepGoing) {
                    break;
                }
            } else {
                //If a step can not be replaced then its the end of optimizationinging.
                break;
            }
        }
    }

    @Override
    protected SqlgStep constructSqlgStep(Step startStep) {
        Preconditions.checkArgument(startStep instanceof GraphStep, "Expected a GraphStep, found instead a " + startStep.getClass().getName());
        GraphStep<?, ?> graphStep = (GraphStep) startStep;
        //noinspection unchecked
        return new SqlgGraphStep(this.sqlgGraph, this.traversal, graphStep.getReturnClass(), graphStep.isStartStep(), graphStep.getIds());
    }

    @Override
    protected boolean doFirst(ListIterator<Step<?, ?>> stepIterator, Step<?, ?> step, MutableInt pathCount) {
        this.currentReplacedStep = ReplacedStep.from(
                this.sqlgGraph.getTopology(),
                (AbstractStep<?, ?>) step,
                pathCount.getValue()
        );
        handleHasSteps(stepIterator, pathCount.getValue());
        handleOrderGlobalSteps(stepIterator, pathCount);
        handleRangeGlobalSteps(stepIterator, pathCount);
        handleConnectiveSteps(stepIterator, pathCount);
        this.sqlgStep = constructSqlgStep(step);
        this.currentTreeNodeNode = this.sqlgStep.addReplacedStep(this.currentReplacedStep);
        replaceStepInTraversal(step, this.sqlgStep);
        if (this.sqlgStep instanceof SqlgGraphStep && ((SqlgGraphStep) this.sqlgStep).getIds().length > 0) {
            addHasContainerForIds((SqlgGraphStep) this.sqlgStep);
        }
        if (!this.currentReplacedStep.hasLabels()) {
            //CountGlobalStep is special, as the select statement will contain no properties
            boolean precedesPathStep = precedesPathOrTreeStep(this.traversal);
            if (precedesPathStep) {
                this.currentReplacedStep.addLabel(pathCount.getValue() + BaseStrategy.PATH_LABEL_SUFFIX + BaseStrategy.SQLG_PATH_FAKE_LABEL);
            }
        }
        pathCount.increment();
        return true;
    }

    @Override
    protected boolean isReplaceableStep(Class<? extends Step> stepClass) {
        return !this.reset && CONSECUTIVE_STEPS_TO_REPLACE.contains(stepClass);
    }

    @Override
    protected void replaceStepInTraversal(Step stepToReplace, SqlgStep sqlgStep) {
        //noinspection unchecked
        TraversalHelper.replaceStep(stepToReplace, sqlgStep, this.traversal);
    }
}
