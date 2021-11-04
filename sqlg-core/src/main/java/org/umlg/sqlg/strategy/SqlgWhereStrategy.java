package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.umlg.sqlg.predicate.ArrayContains;
import org.umlg.sqlg.predicate.ArrayOverlaps;
import org.umlg.sqlg.predicate.FullText;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgTraversalUtil;

import java.util.*;

/**
 * When a Where step uses a FullText, ArrayContains or ArrayOverlaps predicate, we attach it to the step itself as a
 * special hasContainer
 *
 * @author jpmoresmau
 *
 */
public class SqlgWhereStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5608856947238999912L;

	public SqlgWhereStrategy() {
		
	}
	
	@SuppressWarnings("resource")
	@Override
	public void apply(Admin<?, ?> traversal) {
		if (traversal.getGraph().isEmpty() || !(traversal.getGraph().orElseThrow(IllegalStateException::new) instanceof SqlgGraph)) {
			return;
		}
		if (!SqlgTraversalUtil.mayOptimize(traversal)) {
			return;
		}
        SqlgGraph sqlgGraph = (SqlgGraph) traversal.getGraph().get();
        //This is because in normal BatchMode the new vertices are cached with it edges.
        //The query will read from the cache if this is for a cached vertex
        if (sqlgGraph.features().supportsBatchMode() && sqlgGraph.tx().isInNormalBatchMode()) {
            sqlgGraph.tx().flush();
        }
        @SuppressWarnings("unchecked") List<Step<?,?>> steps = new ArrayList(traversal.asAdmin().getSteps());
        ListIterator<Step<?,?>> stepIterator = steps.listIterator();
        // get all steps per label
        Map<String, Object> stepsByLabel=new HashMap<>();
//        stepIterator = steps.listIterator();
        Step<?,?> previous=null;
        int idx=0;
        while (stepIterator.hasNext()) {
            Step<?,?> step = stepIterator.next();
            captureLabels(step, stepsByLabel);
            if (step instanceof WherePredicateStep<?> ){
            	WherePredicateStep<?> wps=(WherePredicateStep<?>)step;
            
	        	if (wps.getPredicate().isPresent()
	        		&& (wps.getPredicate().get().getBiPredicate() instanceof FullText
						|| wps.getPredicate().get().getBiPredicate() instanceof ArrayContains
						|| wps.getPredicate().get().getBiPredicate() instanceof ArrayOverlaps)){
	        		Object referTo=previous;
	        		if (wps.getStartKey().isPresent()){
	        			referTo=stepsByLabel.get(wps.getStartKey().get());
	        		}
	        		if (referTo instanceof SqlgGraphStep<?, ?>){
	        			SqlgGraphStep<?, ?> sgs=(SqlgGraphStep<?, ?>)referTo;
	        			if (sgs.getReplacedSteps().size()>0){
	        				referTo=sgs.getReplacedSteps().get(sgs.getReplacedSteps().size()-1);
	        			}
	        		}
	        		if (referTo instanceof ReplacedStep<?, ?>){
	        			ReplacedStep<?,?> rs=(ReplacedStep<?,?>)referTo;
	        			rs.addHasContainer(new HasContainer("__dummy__", wps.getPredicate().get()));
	        			traversal.removeStep(idx);
	        		}
	        	}
            	
            }
            previous=step;
            idx++;
        }
	}

	/**
	 * add all labels for the step in the given map
	 * @param step
	 * @param stepsByLabel the map to fill up
	 */
	private void captureLabels(Step<?,?> step,Map<String,Object> stepsByLabel){
		for (String s:step.getLabels()){
        	stepsByLabel.put(s, step);
        }
        // labels on replaced steps are not bubbled up to the graphstep
        if (step instanceof SqlgGraphStep<?, ?>){
        	SqlgGraphStep<?, ?> sgs=(SqlgGraphStep<?, ?>)step;
        	for (ReplacedStep<?,?> rs:sgs.getReplacedSteps()){
        		for (String label:rs.getLabels()){
        			 if (label.contains(BaseStrategy.PATH_LABEL_SUFFIX)) {
        				 stepsByLabel.put(label.substring(label.indexOf(BaseStrategy.PATH_LABEL_SUFFIX) + BaseStrategy.PATH_LABEL_SUFFIX.length()),rs);
                     } else if (label.contains(BaseStrategy.EMIT_LABEL_SUFFIX)) {
                    	 stepsByLabel.put(label.substring(label.indexOf(BaseStrategy.EMIT_LABEL_SUFFIX) + BaseStrategy.EMIT_LABEL_SUFFIX.length()),rs);
                     }
        		}
        	}
        }
	}
}
