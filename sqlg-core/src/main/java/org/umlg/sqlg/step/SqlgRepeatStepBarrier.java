package org.umlg.sqlg.step;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/04/20
 */
public class SqlgRepeatStepBarrier<S> extends ComputerAwareStep<S, S> implements TraversalParent {

    private Traversal.Admin<S, S> repeatTraversal = null;
    private Traversal.Admin<S, ?> untilTraversal = null;
    private Traversal.Admin<S, ?> emitTraversal = null;
    public boolean untilFirst = false;
    public boolean emitFirst = false;
    private boolean first = true;
    private List<Traverser.Admin<S>> cachedStarts;
    private List<Iterator<Traverser.Admin<S>>> toReturn;

    public SqlgRepeatStepBarrier(final Traversal.Admin traversal, RepeatStep<S> repeatStep) {
        super(traversal);
        this.repeatTraversal = repeatStep.getRepeatTraversal();
        this.untilTraversal = repeatStep.getUntilTraversal();
        this.emitTraversal = repeatStep.getEmitTraversal();
        this.untilFirst = repeatStep.untilFirst;
        this.emitFirst = repeatStep.emitFirst;
        List<RepeatStep.RepeatEndStep> repeatEndSteps = TraversalHelper.getStepsOfAssignableClass(RepeatStep.RepeatEndStep.class, this.repeatTraversal);
        Preconditions.checkState(repeatEndSteps.size() == 1, "Only handling one RepeatEndStep! Found " + repeatEndSteps.size());
        TraversalHelper.replaceStep(repeatEndSteps.get(0), new SqlgRepeatStepBarrier.SqlgRepeatEndStepBarrier<>(this.repeatTraversal), this.repeatTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = this.getSelfAndChildRequirements(TraverserRequirement.BULK);
        if (requirements.contains(TraverserRequirement.SINGLE_LOOP))
            requirements.add(TraverserRequirement.NESTED_LOOP);
        requirements.add(TraverserRequirement.SINGLE_LOOP);
        return requirements;
    }

    public List<Traversal.Admin<S, S>> getGlobalChildren() {
        return null == this.repeatTraversal ? Collections.emptyList() : Collections.singletonList(this.repeatTraversal);
    }

    public List<Traversal.Admin<S, ?>> getLocalChildren() {
        final List<Traversal.Admin<S, ?>> list = new ArrayList<>(2);
        if (null != this.untilTraversal)
            list.add(this.untilTraversal);
        if (null != this.emitTraversal)
            list.add(this.emitTraversal);
        return list;
    }

    public final boolean doUntil(final Traverser.Admin<S> traverser, boolean utilFirst) {
//        return utilFirst == this.untilFirst && null != this.untilTraversal && TraversalUtil.test(traverser, this.untilTraversal);
        return utilFirst == this.untilFirst && null != this.untilTraversal && test(traverser, this.untilTraversal);
    }

    public final boolean doEmit(final Traverser.Admin<S> traverser, boolean emitFirst) {
//        return emitFirst == this.emitFirst && null != this.emitTraversal && TraversalUtil.test(traverser, this.emitTraversal);
        return emitFirst == this.emitFirst && null != this.emitTraversal && test(traverser, this.emitTraversal);
    }

    @Override
    public String toString() {
        if (this.untilFirst && this.emitFirst)
            return StringFactory.stepString(this, untilString(), emitString(), this.repeatTraversal);
        else if (this.emitFirst)
            return StringFactory.stepString(this, emitString(), this.repeatTraversal, untilString());
        else if (this.untilFirst)
            return StringFactory.stepString(this, untilString(), this.repeatTraversal, emitString());
        else
            return StringFactory.stepString(this, this.repeatTraversal, untilString(), emitString());
    }

    @Override
    public void reset() {
        super.reset();
        if (null != this.emitTraversal)
            this.emitTraversal.reset();
        if (null != this.untilTraversal)
            this.untilTraversal.reset();
        if (null != this.repeatTraversal)
            this.repeatTraversal.reset();
    }

    private final String untilString() {
        return null == this.untilTraversal ? "until(false)" : "until(" + this.untilTraversal + ')';
    }

    private final String emitString() {
        return null == this.emitTraversal ? "emit(false)" : "emit(" + this.emitTraversal + ')';
    }

    /////////////////////////

    @Override
    public SqlgRepeatStepBarrier<S> clone() {
        final SqlgRepeatStepBarrier<S> clone = (SqlgRepeatStepBarrier<S>) super.clone();
        clone.repeatTraversal = this.repeatTraversal.clone();
        if (null != this.untilTraversal)
            clone.untilTraversal = this.untilTraversal.clone();
        if (null != this.emitTraversal)
            clone.emitTraversal = this.emitTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.repeatTraversal);
        this.integrateChild(this.untilTraversal);
        this.integrateChild(this.emitTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.repeatTraversal.hashCode();
        result ^= Boolean.hashCode(this.untilFirst);
        result ^= Boolean.hashCode(this.emitFirst) << 1;
        if (this.untilTraversal != null)
            result ^= this.untilTraversal.hashCode();
        if (this.emitTraversal != null)
            result ^= this.emitTraversal.hashCode();
        return result;
    }

    @Override
    protected Iterator<Traverser.Admin<S>> standardAlgorithm() throws NoSuchElementException {
        while (true) {
            if (this.first) {
                this.first = false;
                this.cachedStarts = new ArrayList<>();
                this.toReturn = new ArrayList<>();
            }
            Iterator<Iterator<Traverser.Admin<S>>> returnIterator = toReturn.iterator();
            if (returnIterator.hasNext()) {
                Iterator<Traverser.Admin<S>> next = returnIterator.next();
                returnIterator.remove();
                return next;
            }
            if (this.repeatTraversal.getEndStep().hasNext()) {
                return this.repeatTraversal.getEndStep();
            } else {
                while (this.starts.hasNext()) {
                    this.cachedStarts.add(this.starts.next());
                }
                if (this.cachedStarts.isEmpty()) {
                    throw FastNoSuchElementException.instance();
                }
                Iterator<Traverser.Admin<S>> iterator = this.cachedStarts.iterator();
                while (iterator.hasNext()) {
                    Traverser.Admin<S> cachedStart = iterator.next();
                    iterator.remove();
                    //optimize this logic to be outside the loop.
                    if (doUntil(cachedStart, true)) {
                        cachedStart.resetLoops();
                        this.toReturn.add(IteratorUtils.of(cachedStart));
                        continue;
                    } else {
                        this.repeatTraversal.addStart(cachedStart);
                    }
                    if (doEmit(cachedStart, true)) {
                        final Traverser.Admin<S> emitSplit = cachedStart.split();
                        emitSplit.resetLoops();
                        this.toReturn.add(IteratorUtils.of(emitSplit));
                    }
                }
            }
        }
    }

    @Override
    protected Iterator<Traverser.Admin<S>> computerAlgorithm() throws NoSuchElementException {
        throw new IllegalStateException("computerAlgorithm not supported!");
    }

    public static class SqlgRepeatEndStepBarrier<S> extends ComputerAwareStep<S, S> {

        private List<Traverser.Admin<S>> cachedStarts;
        private List<Iterator<Traverser.Admin<S>>> toReturn;

        public SqlgRepeatEndStepBarrier(final Traversal.Admin traversal) {
            super(traversal);
            this.cachedStarts = new ArrayList<>();
            this.toReturn = new ArrayList<>();
        }

        @Override
        protected Iterator<Traverser.Admin<S>> standardAlgorithm() throws NoSuchElementException {
            final SqlgRepeatStepBarrier<S> repeatStep = (SqlgRepeatStepBarrier<S>) this.getTraversal().getParent();
            while (true) {
                Iterator<Iterator<Traverser.Admin<S>>> returnIterator = toReturn.iterator();
                if (returnIterator.hasNext()) {
                    Iterator<Traverser.Admin<S>> next = returnIterator.next();
                    returnIterator.remove();
                    return next;
                }
                while (this.starts.hasNext()) {
                    this.cachedStarts.add(this.starts.next());
                }
                if (this.cachedStarts.isEmpty()) {
                    throw FastNoSuchElementException.instance();
                }
                Iterator<Traverser.Admin<S>> iterator = this.cachedStarts.iterator();
                while (iterator.hasNext()) {
                    Traverser.Admin<S> cachedStart = iterator.next();
                    iterator.remove();
                    cachedStart.incrLoops(this.getId());
                    if (repeatStep.doUntil(cachedStart, false)) {
                        cachedStart.resetLoops();
                        toReturn.add(IteratorUtils.of(cachedStart));
                    } else {
                        if (!repeatStep.untilFirst && !repeatStep.emitFirst)
                            repeatStep.repeatTraversal.addStart(cachedStart);
                        else
                            repeatStep.addStart(cachedStart);
                        if (repeatStep.doEmit(cachedStart, false)) {
                            final Traverser.Admin<S> emitSplit = cachedStart.split();
                            emitSplit.resetLoops();
                            toReturn.add(IteratorUtils.of(emitSplit));
                        }
                    }
                }
            }
        }

        @Override
        protected Iterator<Traverser.Admin<S>> computerAlgorithm() throws NoSuchElementException {
            throw new IllegalStateException("computerAlgorithm not supported!");
        }
    }

    public static final <S, E> boolean test(final Traverser.Admin<S> traverser, final Traversal.Admin<S, E> traversal) {
        final Traverser.Admin<S> split = traverser.split();
        if (traversal.getSteps().size() == 1 && traversal.getSteps().get(0) instanceof HasStep) {
            HasStep hasStep = (HasStep) traversal.getSteps().get(0);
            hasStep.reset();
            hasStep.addStart(split);
            return hasStep.hasNext();
        } else {
            split.setSideEffects(traversal.getSideEffects());
            split.setBulk(1l);
            traversal.reset();
            traversal.addStart(split);
            return traversal.hasNext(); // filter
        }
    }
}

