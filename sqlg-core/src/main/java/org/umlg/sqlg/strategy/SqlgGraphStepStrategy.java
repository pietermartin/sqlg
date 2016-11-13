package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Date: 2014/07/12
 * Time: 5:45 AM
 */
public class SqlgGraphStepStrategy extends BaseSqlgStrategy {

    private static final List<Class> CONSECUTIVE_STEPS_TO_REPLACE = Arrays.asList(
            VertexStep.class, EdgeVertexStep.class, GraphStep.class, EdgeOtherVertexStep.class
    );
    private Logger logger = LoggerFactory.getLogger(SqlgVertexStepStrategy.class.getName());

    public SqlgGraphStepStrategy() {
        super();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final Step<?, ?> startStep = traversal.getStartStep();

        //Only optimize graph step.
        if (!(startStep instanceof GraphStep) || !(traversal.getGraph().get() instanceof SqlgGraph)) {
            return;
        }
        SqlgGraph sqlgGraph = (SqlgGraph) traversal.getGraph().get();

        final GraphStep originalGraphStep = (GraphStep) startStep;

        if (sqlgGraph.features().supportsBatchMode() && sqlgGraph.tx().isInNormalBatchMode()) {
            sqlgGraph.tx().flush();
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
        return new SqlgGraphStepCompiled((SqlgGraph) traversal.getGraph().get(), traversal, ((GraphStep) startStep).getReturnClass(), ((GraphStep) startStep).isStartStep(), ((GraphStep) startStep).getIds());
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
        } else {
        	// check if next step isn't a range
            if (stepIterator.hasNext()){
            	step=stepIterator.next();
            	if (!collectRangeGlobalStep(step, stepIterator, traversal, lastReplacedStep,true)){
            		stepIterator.previous();
            	}
            }
        }
    }
    
    /**
     * collect a range global step
     * @param step the current step to collect
     * @param iterator the step iterator
     * @param traversal the traversal of all steps
     * @param replacedStep the current replaced step collecting the info
     * @param multiple are we in a multiple label query?
     * @return true if we impacted the iterator by removing the current step, false otherwise
     */
    private static boolean collectRangeGlobalStep(Step step, ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> replacedStep,boolean multiple){
    	if (step instanceof RangeGlobalStep<?>){
        	RangeGlobalStep<?> rgs=(RangeGlobalStep<?>)step;
        	if (!multiple || rgs.getLowRange()==0){
        		long high=rgs.getHighRange();
        		// when we have multiple labels, we are going to apply the range on the first label first
        		// if we retrieve more than the given range, we don't bother looking at the other labels
        		// so we always ask for one more row here: better to retrieve an extra row and see there's no point
        		// hitting another table
        		if (multiple){
        			high+=1;
        		}
        		replacedStep.setRange(Range.between(rgs.getLowRange(),high ));
        		if (!multiple){
        			iterator.remove();
        			traversal.removeStep(step);
        			return true;
        		}
	            
        	}
    	} 
    	return false;
   
    }

    private static void collectOrderGlobalSteps(Step step, ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> replacedStep) {
        //Collect the OrderGlobalSteps
        if (step instanceof OrderGlobalStep && isElementValueComparator((OrderGlobalStep) step)) {
            iterator.remove();
            traversal.removeStep(step);
            replacedStep.getComparators().addAll(((OrderGlobalStep) step).getComparators());
            // check if next step isn't a range
            if (iterator.hasNext()){
            	step=iterator.next();
            	if (!collectRangeGlobalStep(step, iterator, traversal, replacedStep,false)){
                	iterator.previous();
            	}
            }
        } else if (collectRangeGlobalStep(step, iterator, traversal, replacedStep,false)){
        	// noop
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
            } else if (collectRangeGlobalStep(currentStep, iterator, traversal, replacedStep,false)){
            	// noop
            } else {
                iterator.previous();
                break;
            }
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPost() {
        return Stream.of(InlineFilterStrategy.class).collect(Collectors.toSet());
    }
}

