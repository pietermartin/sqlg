package org.umlg.sqlg.step;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.HasNextStep;

import java.util.List;
import java.util.Map;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/04/24
 */
public class SqlgChooseStepBarrier<S, E, M> extends SqlgBranchStepBarrier<S, E, M> {

    public SqlgChooseStepBarrier(final Traversal.Admin traversal, final Traversal.Admin<S, M> choiceTraversal) {
        super(traversal);
        this.setBranchTraversal(choiceTraversal);
    }

    public SqlgChooseStepBarrier(final Traversal.Admin traversal, final Traversal.Admin<S, ?> predicateTraversal, final Traversal.Admin<S, E> trueChoice, final Traversal.Admin<S, E> falseChoice) {
        this(traversal, (Traversal.Admin<S, M>) predicateTraversal);
        this.addGlobalChildOption((M) Boolean.TRUE, trueChoice);
        this.addGlobalChildOption((M) Boolean.FALSE, falseChoice);
        //remove the HasNextStep
        HasNextStep hasNextStep = null;
        for (Step step : predicateTraversal.getSteps()) {
            if (step instanceof HasNextStep) {
                hasNextStep = (HasNextStep) step;
            }
        }
        Preconditions.checkState(hasNextStep != null);
        predicateTraversal.removeStep(hasNextStep);
    }

    public Map<M, List<Traversal.Admin<S, E>>> getTraversalOptions() {
        return this.traversalOptions;
    }

    @Override
    public void addGlobalChildOption(final M pickToken, final Traversal.Admin<S, E> traversalOption) {
        if (Pick.any.equals(pickToken))
            throw new IllegalArgumentException("Choose step can not have an any-option as only one option per traverser is allowed");
        if (this.traversalOptions.containsKey(pickToken))
            throw new IllegalArgumentException("Choose step can only have one traversal per pick token: " + pickToken);
        super.addGlobalChildOption(pickToken, traversalOption);
    }
}
