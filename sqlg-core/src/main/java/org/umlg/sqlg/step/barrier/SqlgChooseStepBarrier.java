package org.umlg.sqlg.step.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/04/24
 */
public class SqlgChooseStepBarrier<S, E, M> extends SqlgBranchStepBarrier<S, E, M> {

    public SqlgChooseStepBarrier(final Traversal.Admin traversal, final Traversal.Admin<S, M> choiceTraversal) {
        super(traversal);
        this.setBranchTraversal(choiceTraversal);
    }

    @Override
    public void addGlobalChildOption(final M pickToken, final Traversal.Admin<S, E> traversalOption) {
        if (pickToken instanceof Pick) {
            if (Pick.any.equals(pickToken))
                throw new IllegalArgumentException("Choose step can not have an any-option as only one option per traverser is allowed");
            if (this.traversalPickOptions.containsKey(pickToken))
                throw new IllegalArgumentException("Choose step can only have one traversal per pick token: " + pickToken);
        }
        super.addGlobalChildOption(pickToken, traversalOption);
    }

}
