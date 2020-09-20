package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.function.Predicate;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2020/09/13
 */
public class SqlgLambdaFilterStep<S> extends SqlgFilterStep<S> {

    private final Predicate<Traverser<S>> predicate;

    public SqlgLambdaFilterStep(final Traversal.Admin traversal, final Predicate<Traverser<S>> predicate) {
        super(traversal);
        this.predicate = predicate;
    }

    public Predicate<Traverser<S>> getPredicate() {
        return predicate;
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        return this.predicate.test(traverser);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.predicate);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.predicate.hashCode();
    }

}
