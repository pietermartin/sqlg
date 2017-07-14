package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.MessagePassingReductionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.*;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2014/08/15
 */
public class SqlgVertexStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy  {


    public SqlgVertexStepStrategy() {
        super();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        //Only optimize SqlgGraph. StarGraph also passes through here.
        //noinspection OptionalGetWithoutIsPresent
        if (!(traversal.getGraph().get() instanceof SqlgGraph)) {
            return;
        }
        VertexStrategy.from(traversal).apply();
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPost() {
        return Stream.of(
                MatchPredicateStrategy.class,
                RepeatUnrollStrategy.class,
                PathRetractionStrategy.class,
                InlineFilterStrategy.class,
                MessagePassingReductionStrategy.class,
                IncidentToAdjacentStrategy.class
        ).collect(Collectors.toSet());
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return Stream.of(SqlgGraphStepStrategy.class).collect(Collectors.toSet());
    }

}
