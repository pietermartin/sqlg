package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
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

    private static final List<Class> CONSECUTIVE_STEPS_TO_REPLACE = Arrays.asList(
            VertexStep.class, EdgeVertexStep.class, GraphStep.class, EdgeOtherVertexStep.class, ChooseStep.class
    );
    private Logger logger = LoggerFactory.getLogger(SqlgVertexStepStrategy.class.getName());

    public SqlgGraphStepStrategy() {
        super();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final Step<?, ?> startStep = traversal.getStartStep();

        //Only optimize graph step.
        if (!(startStep instanceof GraphStep)) {
            return;
        }
        this.sqlgGraph = (SqlgGraph) traversal.getGraph().get();

        final GraphStep originalGraphStep = (GraphStep) startStep;

        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInNormalBatchMode()) {
            this.sqlgGraph.tx().flush();
        }

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

    @Override
    protected SqlgStep constructSqlgStep(Traversal.Admin<?, ?> traversal, Step startStep) {
        Preconditions.checkArgument(startStep instanceof GraphStep, "Expected a GraphStep, found instead a " + startStep.getClass().getName());
        return new SqlgGraphStepCompiled(this.sqlgGraph, traversal, ((GraphStep) startStep).getReturnClass(), ((GraphStep) startStep).isStartStep(), ((GraphStep) startStep).getIds());
    }

    @Override
    protected ReplacedStep getPreviousReplacedStep(SqlgStep sqlgStep) {
        List<ReplacedStep> previousReplacedSteps = sqlgStep.getReplacedSteps();
        return previousReplacedSteps.get(previousReplacedSteps.size() - 1);
    }

    @Override
    protected boolean isReplaceableStep(Class<? extends Step> stepClass, boolean alreadyReplacedGraphStep) {
        return CONSECUTIVE_STEPS_TO_REPLACE.contains(stepClass) && !(stepClass.isAssignableFrom(GraphStep.class) && alreadyReplacedGraphStep);
    }

    @Override
    protected void replaceStepInTraversal(Step firstStep, SqlgStep sqlgStep, Traversal.Admin<?, ?> traversal) {
        TraversalHelper.replaceStep(firstStep, sqlgStep, traversal);
    }

    @Override
    protected void doLastEntry(Step step, ListIterator<Step> stepIterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> lastReplacedStep, SqlgStep sqlgStep) {
        Preconditions.checkArgument(lastReplacedStep != null);
        //TODO optimize this, to not parse if there are no OrderGlobalSteps
        sqlgStep.parseForStrategy();
        if (!sqlgStep.isForMultipleQueries()) {
            collectOrderGlobalSteps(step, stepIterator, traversal, lastReplacedStep);
        }
    }

    private static void collectOrderGlobalSteps(Step step, ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> replacedStep) {
        //Collect the OrderGlobalSteps
        if (step instanceof OrderGlobalStep && isElementValueComparator((OrderGlobalStep) step)) {
            iterator.remove();
            traversal.removeStep(step);
            replacedStep.getComparators().addAll(((OrderGlobalStep) step).getComparators());
        } else {
            collectSelectOrderGlobalSteps(iterator, traversal, replacedStep);
        }
    }

    private static void collectSelectOrderGlobalSteps(ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> replacedStep) {
        //Collect the OrderGlobalSteps
        while (iterator.hasNext()) {
            Step<?, ?> currentStep = iterator.next();
            if (currentStep instanceof OrderGlobalStep && (isElementValueComparator((OrderGlobalStep) currentStep) || isTraversalComparatorWithSelectOneStep((OrderGlobalStep) currentStep))) {
                iterator.remove();
                traversal.removeStep(currentStep);
                replacedStep.getComparators().addAll(((OrderGlobalStep) currentStep).getComparators());
            } else if (currentStep instanceof IdentityStep) {
                // do nothing
            } else {
                iterator.previous();
                break;
            }
        }
    }
}

