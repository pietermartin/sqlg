package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;

/**
 * Created by pieter on 2015/12/09.
 */
public class TopologyStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    private final List<String> selectFrom;
    private final List<String> selectWithout;
    public static final String TOPOLOGY_SELECTION_FROM = "~~TopologySelectionFrom~~";
    public static final String TOPOLOGY_SELECTION_WITHOUT = "~~TopologySelectionWithout~~";

    private TopologyStrategy(final Builder builder) {
        this.selectFrom = builder.selectFrom;
        this.selectWithout = builder.selectWithout;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (this.selectFrom != null) {
            TraversalHelper.insertAfterStep(
                    new HasStep(traversal, new HasContainer(TOPOLOGY_SELECTION_FROM, P.within(this.selectFrom))),
                    traversal.getStartStep(),
                    traversal
            );
        }
        if (this.selectWithout != null) {
            TraversalHelper.insertAfterStep(
                    new HasStep(traversal, new HasContainer(TOPOLOGY_SELECTION_WITHOUT, P.within(this.selectWithout))),
                    traversal.getStartStep(),
                    traversal
            );
        }
    }

    public static Builder build() {
        return new Builder();
    }

    public final static class Builder {

        private List<String> selectFrom;
        private List<String> selectWithout;

        public TopologyStrategy create() {
            return new TopologyStrategy(this);
        }

        public Builder selectFrom(List<String> selectFrom) {
            if (this.selectWithout != null) {
                throw new IllegalStateException("selectWithout and selectFrom are mutually exclusive. selectWithout is already set.");
            }
            this.selectFrom = selectFrom;
            return this;
        }

        public Builder selectWithout(List<String> selectWithout) {
            if (this.selectFrom != null) {
                throw new IllegalStateException("selectWithout and selectFrom are mutually exclusive. selectFrom is already set.");
            }
            this.selectWithout = selectWithout;
            return this;
        }

    }
}
