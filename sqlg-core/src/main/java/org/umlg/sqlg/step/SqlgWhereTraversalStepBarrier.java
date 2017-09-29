package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;

import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/09/28
 */
public class SqlgWhereTraversalStepBarrier<S> extends AbstractStep<S, S> implements TraversalParent, Scoping, PathProcessor {

    public SqlgWhereTraversalStepBarrier(Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    public void setKeepLabels(Set<String> labels) {

    }

    @Override
    public Set<String> getKeepLabels() {
        return null;
    }

    @Override
    public Set<String> getScopeKeys() {
        return null;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        return null;
    }
}
