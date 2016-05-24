package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Date: 2014/08/15
 * Time: 7:34 PM
 */
public class SqlgVertexStepStrategy extends BaseSqlgStrategy {

    private static final List<Class> CONSECUTIVE_STEPS_TO_REPLACE = Arrays.asList(VertexStep.class, EdgeVertexStep.class, EdgeOtherVertexStep.class);
//    private static final List<Class> CONSECUTIVE_STEPS_TO_REPLACE = Arrays.asList(VertexStep.class, EdgeVertexStep.class);
    private Logger logger = LoggerFactory.getLogger(SqlgVertexStepStrategy.class.getName());

    public SqlgVertexStepStrategy() {
        super();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        this.sqlgGraph = (SqlgGraph) traversal.getGraph().get();
        //This is because in normal BatchMode the new vertices are cached with it edges.
        //The query will read from the cache if this is for a cached vertex
        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInNormalBatchMode()) {
            this.sqlgGraph.tx().flush();
        }
        List<Step> steps = new ArrayList<>(traversal.asAdmin().getSteps());
        ListIterator<Step> stepIterator = steps.listIterator();
        if (this.canNotBeOptimized(steps, stepIterator.nextIndex())) {
            logger.debug("gremlin not optimized due to path or tree step. " + traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
            return;
        }
        combineSteps(traversal, steps, stepIterator);

    }

    @Override
    protected SqlgStep constructSqlgStep(Traversal.Admin<?, ?> traversal, Step startStep) {
        SqlgVertexStepCompiled sqlgStep = new SqlgVertexStepCompiled(traversal);
        ReplacedStep replacedStep = ReplacedStep.from(this.sqlgGraph.getSchemaManager());
        sqlgStep.addReplacedStep(replacedStep);
        return sqlgStep;
    }

    @Override
    protected boolean isReplaceableStep(Class<? extends Step> stepClass, boolean alreadyReplacedGraphStep) {
        return CONSECUTIVE_STEPS_TO_REPLACE.contains(stepClass);
    }

    @Override
    protected void replaceStepInTraversal(Step stepToReplace, SqlgStep sqlgStep, Traversal.Admin<?, ?> traversal) {
        if (traversal.getSteps().contains(stepToReplace)) {
            TraversalHelper.replaceStep(stepToReplace, sqlgStep, traversal);
        } else {
            TraversalHelper.insertAfterStep(sqlgStep, stepToReplace.getPreviousStep(), traversal);
        }
    }

    @Override
    protected void doLastEntry(Step step, ListIterator<Step> stepIterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> lastReplacedStep, SqlgStep sqlgStep) {
        Preconditions.checkArgument(lastReplacedStep != null);
        replaceOrderGlobalSteps(step, stepIterator, traversal, lastReplacedStep);
    }


    private static void replaceOrderGlobalSteps(Step step, ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> replacedStep) {
        //Collect the OrderGlobalSteps
        if (step instanceof OrderGlobalStep && isElementValueComparator((OrderGlobalStep) step)) {
            TraversalHelper.replaceStep(step, new SqlgOrderGlobalStep<>((OrderGlobalStep) step), traversal);
            iterator.remove();
            replacedStep.getComparators().addAll(((OrderGlobalStep) step).getComparators());
        } else {
            replaceSelectOrderGlobalSteps(iterator, traversal, replacedStep);
        }
    }

    private static void replaceSelectOrderGlobalSteps(ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> replacedStep) {
        //Collect the OrderGlobalSteps
        while (iterator.hasNext()) {
            Step currentStep = iterator.next();
//            if (currentStep instanceof OrderGlobalStep && (isElementValueComparator((OrderGlobalStep) currentStep) || isTraversalComparatorWithSelectOneStep((OrderGlobalStep) currentStep))) {
            if (currentStep instanceof OrderGlobalStep && (isElementValueComparator((OrderGlobalStep) currentStep) )) {
                iterator.remove();
                TraversalHelper.replaceStep(currentStep, new SqlgOrderGlobalStep<>((OrderGlobalStep) currentStep), traversal);
                replacedStep.getComparators().addAll(((OrderGlobalStep) currentStep).getComparators());
            } else if (currentStep instanceof IdentityStep) {
                // do nothing
            } else {
                iterator.previous();
                break;
            }
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return Stream.of(SqlgGraphStepStrategy.class).collect(Collectors.toSet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        SqlgVertexStepStrategy that = (SqlgVertexStepStrategy) o;

        return logger != null ? logger.equals(that.logger) : that.logger == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (logger != null ? logger.hashCode() : 0);
        return result;
    }
}
