package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.umlg.sqlg.strategy.SqlgNoSuchElementException;
import org.umlg.sqlg.strategy.SqlgResetTraversalException;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/04/20
 */
public class SqlgLocalStep<S, E> extends AbstractStep<S, E> implements TraversalParent, Step<S, E> {

    private Traversal.Admin<S, E> localTraversal;

    public SqlgLocalStep(final Traversal.Admin traversal, LocalStep<S, E> localStep) {
        super(traversal);
        this.localTraversal = localStep.getLocalChildren().get(0);
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return Collections.singletonList(this.localTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.localTraversal.getTraverserRequirements();
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        while (this.starts.hasNext()) {
            this.localTraversal.addStart(this.starts.next());
        }
        while (true) {
            try {
                if (this.localTraversal.hasNext()) {
                    Traverser.Admin<E> traverser = this.localTraversal.nextTraverser();
                    return traverser;
                }
            } catch (SqlgResetTraversalException e) {
                this.localTraversal.reset();
            } catch (SqlgNoSuchElementException e) {
                throw FastNoSuchElementException.instance();
            }
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.localTraversal.reset();
    }

    @Override
    public SqlgLocalStep<S, E> clone() {
        final SqlgLocalStep<S, E> clone = (SqlgLocalStep<S, E>) super.clone();
        clone.localTraversal = this.localTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.localTraversal);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.localTraversal.hashCode();
    }
}
