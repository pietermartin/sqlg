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
    private static final long serialVersionUID = -2324356589627718575L;

    @SuppressWarnings("resource")
    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (traversal.getGraph().isEmpty() || !(traversal.getGraph().orElseThrow(IllegalStateException::new) instanceof SqlgGraph)) {
            return;
        }
        if (!SqlgTraversalUtil.mayOptimize(traversal)) {
            return;
        }
        SqlgGraph sqlgGraph = (SqlgGraph) traversal.getGraph().orElseThrow(IllegalStateException::new);
        //This is because in normal BatchMode the new vertices are cached with it edges.
        //The query will read from the cache if this is for a cached vertex
        if (sqlgGraph.features().supportsBatchMode() && sqlgGraph.tx().isInNormalBatchMode()) {
            sqlgGraph.tx().flush();
        }
        @SuppressWarnings("unchecked") List<Step<?, ?>> steps = new ArrayList(traversal.asAdmin().getSteps());
        ListIterator<Step<?, ?>> stepIterator = steps.listIterator();
        Step<?, ?> previous = null;
        while (stepIterator.hasNext()) {
            Step<?, ?> step = stepIterator.next();
            Collection<String> restrict = getRestrictedProperties(step);
            if (restrict != null) {

                Object referTo = previous;
                if (referTo instanceof SqlgGraphStep<?, ?>) {
                    SqlgGraphStep<?, ?> sgs = (SqlgGraphStep<?, ?>) referTo;
                    if (sgs.getReplacedSteps().size() > 0) {
                        referTo = sgs.getReplacedSteps().get(sgs.getReplacedSteps().size() - 1);
                    }
                }
                if (referTo instanceof ReplacedStep<?, ?>) {
                    ReplacedStep<?, ?> rs = (ReplacedStep<?, ?>) referTo;
                    if (rs.getRestrictedProperties() == null) {
                        rs.setRestrictedProperties(new HashSet<>());
                    }
                    rs.getRestrictedProperties().addAll(restrict);
                }


            }
            previous = step;
        }

    }

    private Collection<String> getRestrictedProperties(Step<?, ?> step) {
        Collection<String> ret = null;
        if (step instanceof PropertiesStep<?>) {
            PropertiesStep<?> ps = (PropertiesStep<?>) step;
            ret = Arrays.asList(ps.getPropertyKeys());
        } else if (step instanceof PropertyMapStep<?, ?>) {
            PropertyMapStep<?, ?> pms = (PropertyMapStep<?, ?>) step;
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
