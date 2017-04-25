package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.HasNextStep;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/04/24
 */
public class SqlgChooseStep<S, E, M> extends SqlgBranchStep<S, E, M> {

    public SqlgChooseStep(final Traversal.Admin traversal, final Traversal.Admin<S, M> choiceTraversal) {
        super(traversal);
        this.setBranchTraversal(choiceTraversal);
    }

    public SqlgChooseStep(final Traversal.Admin traversal, final Traversal.Admin<S, ?> predicateTraversal, final Traversal.Admin<S, E> trueChoice, final Traversal.Admin<S, E> falseChoice) {
        this(traversal, (Traversal.Admin<S, M>) predicateTraversal.addStep(new HasNextStep<>(predicateTraversal)));
        this.addGlobalChildOption((M) Boolean.TRUE, trueChoice);
        this.addGlobalChildOption((M) Boolean.FALSE, falseChoice);
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
