package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.step.SqlgStep;
import org.umlg.sqlg.step.SqlgVertexStep;
import org.umlg.sqlg.strategy.barrier.SqlgVertexStepStrategy;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgTraversalUtil;

import java.io.Serial;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This strategy will look for SelectOneStep followed by a PropertyMap step, i.e. `.select('x').values('y')` as a localChild
 * If it finds it and the ReplacedStep already has retricted properties then it will append the values label to the
 * restricted properties of the `SqlgGraphStep` or `SqlgVertexStep`
 *
 *
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2023/08/10
 */
public class SqlgSelectOneStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

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
        SqlgStep previous = null;
        for (Step<?, ?> step : steps) {
            if (step instanceof SqlgGraphStep<?, ?> sqlgGraphStep) {
                previous = sqlgGraphStep;
            } else if (step instanceof SqlgVertexStep<?> sqlgVertexStep) {
                previous = sqlgVertexStep;
            }
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> localChild : ((TraversalParent) step).getLocalChildren()) {
                    List<SelectOneStep> selectOneSteps = TraversalHelper.getStepsOfAssignableClass(SelectOneStep.class, localChild);
                    for (SelectOneStep selectOneStep : selectOneSteps) {
                        if (selectOneStep.getNextStep() instanceof PropertiesStep<?> propertiesStep) {
                            for (String propertyKey : propertiesStep.getPropertyKeys()) {
                                ReplacedStep replacedStep = previous.getReplacedSteps().get(previous.getReplacedSteps().size() - 1);
                                if (!replacedStep.getRestrictedProperties().isEmpty()) {
                                    replacedStep.getRestrictedProperties().add(propertyKey);
                                }
                            }
                        }
                    }
                }
                for (final Traversal.Admin<?, ?> globalChild : ((TraversalParent) step).getGlobalChildren()) {
                    List<SelectOneStep> selectOneSteps = TraversalHelper.getStepsOfAssignableClass(SelectOneStep.class, globalChild);
                    for (SelectOneStep selectOneStep : selectOneSteps) {
                        if (selectOneStep.getNextStep() instanceof PropertiesStep<?> propertiesStep) {
                            for (String propertyKey : propertiesStep.getPropertyKeys()) {
                                ReplacedStep replacedStep = previous.getReplacedSteps().get(previous.getReplacedSteps().size() - 1);
                                if (!replacedStep.getRestrictedProperties().isEmpty()) {
                                    replacedStep.getRestrictedProperties().add(propertyKey);
                                }
                            }
                        }
                    }
                }
            }
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
