package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * Created by pieter on 2015/12/09.
 */
public class TopologyStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    private boolean sqlgSchema;
    private boolean globalUniqueIndex;
    public static final String TOPOLOGY_SELECTION_SQLG_SCHEMA = "~~TopologySelectionSqlgSchema~~";
    public static final String TOPOLOGY_SELECTION_GLOBAL_UNIQUE_INDEX = "~~TopologySelectionGlobalUniqueIndex~~";

    private TopologyStrategy(final Builder builder) {
        this.sqlgSchema = builder.sqlgSchema;
        this.globalUniqueIndex = builder.globalUniqueIndex;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        Preconditions.checkState(!(this.sqlgSchema && this.globalUniqueIndex), "sqlgSchema and globalUnique are mutually exclusive. Both can not be true.");
        if (this.sqlgSchema) {
            TraversalHelper.insertAfterStep(
                    new HasStep(traversal, new HasContainer(TOPOLOGY_SELECTION_SQLG_SCHEMA, P.eq(this.sqlgSchema))),
                    traversal.getStartStep(),
                    traversal
            );
        }
        if (this.globalUniqueIndex) {
            TraversalHelper.insertAfterStep(
                    new HasStep(traversal, new HasContainer(TOPOLOGY_SELECTION_GLOBAL_UNIQUE_INDEX, P.eq(this.globalUniqueIndex))),
                    traversal.getStartStep(),
                    traversal
            );
        }
    }

    public static Builder build() {
        return new Builder();
    }

    public final static class Builder {

        private boolean sqlgSchema;
        private boolean globalUniqueIndex;

        public TopologyStrategy create() {
            return new TopologyStrategy(this);
        }

        public Builder sqlgSchema() {
            if (this.globalUniqueIndex) {
                throw new IllegalStateException("sqlgSchema and globalUniqueIndex are mutually exclusive. globalUniqueIndex is already set.");
            }
            this.sqlgSchema = true;
            return this;
        }

        public Builder globallyUniqueIndexes() {
            if (this.sqlgSchema) {
                throw new IllegalStateException("sqlgSchema and globalUniqueIndex are mutually exclusive. sqlgSchema is already set.");
            }
            this.globalUniqueIndex = true;
            return this;
        }

    }
}
