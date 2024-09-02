package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.strategy.barrier.SqlgVertexStepStrategy;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgTraversalUtil;

import java.io.Serial;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Restricts the set of properties to read for a given vertex or edge, if we determine that we don't need them all for the rest of the traversal
 *
 * @author JP Moresmau
 */
public class SqlgRestrictPropertiesStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    /**
     *
     */
    @Serial
    private static final long serialVersionUID = -2324356589627718575L;

    @SuppressWarnings("resource")
    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (traversal.getGraph().isEmpty() || !(traversal.getGraph().orElseThrow(IllegalStateException::new) instanceof SqlgGraph sqlgGraph)) {
            return;
        }
        if (!SqlgTraversalUtil.mayOptimize(traversal)) {
            return;
        }
        //This is because in normal BatchMode the new vertices are cached with it edges.
        //The query will read from the cache if this is for a cached vertex
        if (sqlgGraph.features().supportsBatchMode() && sqlgGraph.tx().isInNormalBatchMode()) {
            sqlgGraph.tx().flush();
        }
        List<Step<?, ?>> steps = new ArrayList(traversal.asAdmin().getSteps());
        ListIterator<Step<?, ?>> stepIterator = steps.listIterator();
        Step<?, ?> previous = null;
        while (stepIterator.hasNext()) {
            Step<?, ?> step = stepIterator.next();
            Collection<String> restrict = getRestrictedProperties(step);
            if (restrict != null) {

                Object referTo = previous;
                if (referTo instanceof SqlgGraphStep<?, ?> sqlgGraphStep) {
                    if (!sqlgGraphStep.getReplacedSteps().isEmpty()) {
                        referTo = sqlgGraphStep.getReplacedSteps().get(sqlgGraphStep.getReplacedSteps().size() - 1);
                    }
                }
                if (referTo instanceof ReplacedStep<?, ?> replacedStep) {
                    replacedStep.getRestrictedProperties().addAll(restrict);
                }

            }
            previous = step;
        }

    }

    private Collection<String> getRestrictedProperties(Step<?, ?> step) {
        Collection<String> ret = null;
        if (step instanceof PropertiesStep<?> ps) {
            ret = Arrays.asList(ps.getPropertyKeys());
        } else if (step instanceof PropertyMapStep<?, ?> pms) {
            ret = Arrays.asList(pms.getPropertyKeys());
        }
        // if no property keys are provided, all properties should be returned
        if (ret != null && ret.isEmpty()) {
            ret = null;
        }

        return ret;
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return Stream.of(
                SqlgGraphStepStrategy.class,
                SqlgVertexStepStrategy.class
        ).collect(Collectors.toSet());
    }

}
