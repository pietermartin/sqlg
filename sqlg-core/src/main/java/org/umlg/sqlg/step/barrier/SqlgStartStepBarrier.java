package org.umlg.sqlg.step.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.umlg.sqlg.step.SqlgAbstractStep;
import org.umlg.sqlg.structure.traverser.SqlgTraverserGenerator;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2019/08/12
 */
public class SqlgStartStepBarrier<S> extends SqlgAbstractStep<S, S> implements TraversalParent, Step<S, S> {

    protected Object start;
    private boolean first = true;
    private final List<Traverser.Admin<S>> results = new ArrayList<>();
    private Iterator<Traverser.Admin<S>> resultIterator;

    public SqlgStartStepBarrier(final Traversal.Admin traversal) {
        super(traversal);
    }

    public SqlgStartStepBarrier(final Traversal.Admin traversal, StartStep<S> startStep) {
        super(traversal);
        this.start = startStep.getStart();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Traversal.Admin<S, TraversalOptionParent.Pick>> getLocalChildren() {
        return Collections.emptyList();
    }

    @Override
    public List<Traversal.Admin<S, S>> getGlobalChildren() {
        return Collections.emptyList();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return new HashSet<>();
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        if (this.first) {
            if (null != this.start) {
                if (this.start instanceof Iterator) {
                    Iterator<Traverser.Admin<S>> traversers = SqlgTraverserGenerator.instance().generateIterator(
                            (Iterator<S>)this.start,
                            this,
                            1L, false, false);
                    this.starts.add(traversers);
                } else {
                    //TODO, this never gets called, investigate
//                    this.starts.add(this.getTraversal().getTraverserGenerator().generate((S) this.start, this, 1l));
                    Traverser.Admin<S> traverser = SqlgTraverserGenerator.instance().generate(
                            (S)this.start,
                            this,
                            1L, false, false);
                    this.starts.add(traverser);
                }
            }
            this.first = false;
        }
        ///
        final Traverser.Admin<S> start = this.starts.next();
        if (start.get() instanceof Attachable &&
                this.getTraversal().getGraph().isPresent() &&
                (!(start.get() instanceof VertexProperty) || null != ((VertexProperty) start.get()).element()))
            start.set(((Attachable<S>) start.get()).attach(Attachable.Method.get(this.getTraversal().getGraph().get())));
        return start;
    }

    @Override
    public void reset() {
        this.first = true;
        this.results.clear();
    }

    @Override
    public SqlgStartStepBarrier<S> clone() {
        final SqlgStartStepBarrier<S> clone = (SqlgStartStepBarrier<S>) super.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.start);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
