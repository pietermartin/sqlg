package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * Created by pieter on 2015/12/09.
 */
public class TopologyStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    private final boolean sqlgSchema;
    public static final String TOPOLOGY_SELECTION_SQLG_SCHEMA = "~~TopologySelectionSqlgSchema~~";
    public static final String TOPOLOGY_SELECTION_GLOBAL_UNIQUE_INDEX = "~~TopologySelectionGlobalUniqueIndex~~";

    private TopologyStrategy(final Builder builder) {
        this.sqlgSchema = builder.sqlgSchema;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (!TraversalHelper.getStepsOfAssignableClass(GraphStep.class, traversal).isEmpty() || !TraversalHelper.getStepsOfAssignableClass(VertexStep.class, traversal).isEmpty()) {
            if (this.sqlgSchema) {
                TraversalHelper.insertAfterStep(
                        new HasStep(traversal, new HasContainer(TOPOLOGY_SELECTION_SQLG_SCHEMA, P.eq(this.sqlgSchema))),
                        traversal.getStartStep(),
                        traversal
                );
            }
        }

    }

    public static Builder build() {
        return new Builder();
    }

    public final static class Builder {

        private boolean sqlgSchema;

        public TopologyStrategy create() {
            return new TopologyStrategy(this);
        }

        public Builder sqlgSchema() {
            this.sqlgSchema = true;
            return this;
        }

    }
}
