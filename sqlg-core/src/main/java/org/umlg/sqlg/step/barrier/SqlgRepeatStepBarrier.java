package org.umlg.sqlg.step.barrier;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.umlg.sqlg.step.SqlgAbstractStep;
import org.umlg.sqlg.step.SqlgComputerAwareStep;
import org.umlg.sqlg.step.SqlgExpandableStepIterator;
import org.umlg.sqlg.strategy.SqlgRangeHolder;

import java.lang.reflect.Field;
import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/04/20
 */
public class SqlgRepeatStepBarrier<S> extends SqlgComputerAwareStep<S, S> implements TraversalParent {

    private Traversal.Admin<S, S> repeatTraversal = null;
    private Traversal.Admin<S, ?> untilTraversal = null;
    private Traversal.Admin<S, ?> emitTraversal = null;
    private boolean untilFirst = false;
    private boolean emitFirst = false;
    private boolean first = true;
    private List<Iterator<Traverser.Admin<S>>> toReturn;
    //Special cache for untilTraverser
    private List<HasContainer> untilHasContainers;
    private HasContainer untilHasContainer;
    //This is for limit/range step immediately following the Repeat.
    //It needs to be embedded else the repeat might just repeat forever.
    private SqlgRangeHolder sqlgRangeHolder;
    private long rangeCount = 0;

    private String loopName = null;

    @SuppressWarnings("unchecked")
    public SqlgRepeatStepBarrier(final Traversal.Admin traversal, RepeatStep<S> repeatStep) {
        super(traversal);
        this.repeatTraversal = repeatStep.getRepeatTraversal();
        this.untilTraversal = repeatStep.getUntilTraversal();
        this.emitTraversal = repeatStep.getEmitTraversal();
        this.untilFirst = repeatStep.untilFirst;
        this.emitFirst = repeatStep.emitFirst;
        try {
            Field f = repeatStep.getClass().getDeclaredField("loopName");
            f.setAccessible(true);
            this.loopName = (String)f.get(repeatStep);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        List<RepeatStep.RepeatEndStep> repeatEndSteps = TraversalHelper.getStepsOfAssignableClass(RepeatStep.RepeatEndStep.class, this.repeatTraversal);
        Preconditions.checkState(repeatEndSteps.size() == 1, "Only handling one RepeatEndStep! Found " + repeatEndSteps.size());
        SqlgRepeatEndStepBarrier sqlgRepeatEndStepBarrier = new SqlgRepeatEndStepBarrier(this.repeatTraversal);
        TraversalHelper.replaceStep(repeatEndSteps.get(0), sqlgRepeatEndStepBarrier, this.repeatTraversal);
//        sqlgRepeatEndStepBarrier.setId(repeatEndSteps.get(0).getId());
    }

    public void setSqlgRangeHolder(SqlgRangeHolder sqlgRangeHolder) {
        this.sqlgRangeHolder = sqlgRangeHolder;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = this.getSelfAndChildRequirements(TraverserRequirement.BULK);
        if (requirements.contains(TraverserRequirement.SINGLE_LOOP))
            requirements.add(TraverserRequirement.NESTED_LOOP);
        requirements.add(TraverserRequirement.SINGLE_LOOP);
        return requirements;
    }

    @SuppressWarnings("unchecked")
    public List<Traversal.Admin<S, S>> getGlobalChildren() {
        return null == this.repeatTraversal ? Collections.emptyList() : Collections.singletonList(this.repeatTraversal);
    }

    @SuppressWarnings("unchecked")
    public List<Traversal.Admin<S, ?>> getLocalChildren() {
        final List<Traversal.Admin<S, ?>> list = new ArrayList<>(2);
        if (null != this.untilTraversal)
            list.add(this.untilTraversal);
        if (null != this.emitTraversal)
            list.add(this.emitTraversal);
        return list;
    }

    private boolean doUntil(final Traverser.Admin<S> traverser, boolean utilFirst) {
        return utilFirst == this.untilFirst && null != this.untilTraversal && TraversalUtil.test(traverser, this.untilTraversal);
    }

    private boolean doEmit(final Traverser.Admin<S> traverser, boolean emitFirst) {
        return emitFirst == this.emitFirst && null != this.emitTraversal && TraversalUtil.test(traverser, this.emitTraversal);
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

    private String untilString() {
        return null == this.untilTraversal ? "until(false)" : "until(" + this.untilTraversal + ')';
    }

    private String emitString() {
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
                this.toReturn = new ArrayList<>();
            }
            Iterator<Iterator<Traverser.Admin<S>>> returnIterator = toReturn.iterator();
            if (returnIterator.hasNext()) {
                Iterator<Traverser.Admin<S>> next = returnIterator.next();
                returnIterator.remove();
                if (this.sqlgRangeHolder != null && applyRange()) {
                    continue;
                }
                return next;
            }
            if (this.repeatTraversal.getEndStep().hasNext()) {
                return this.repeatTraversal.getEndStep();
            } else {
                boolean foundSomething = false;
//                if (this.optimizeUntil) {
                if (barrierUntil()) {
                    if (this.untilFirst) {
                        Multimap<String, Traverser.Admin<S>> startsToContinue = LinkedListMultimap.create();
                        //doUntilBarrier will iterate the starts and for each the utilTraversal.
                        //The starts for which the untilTraversal returns something will be placed in the toReturn list.
                        //The rest will be in the startsToContinue map.
                        //They are then added to the repeatTraversal to continue the repetition.
                        foundSomething = doUntilBarrier(this.starts, this.toReturn, startsToContinue);
                        for (Traverser.Admin<S> start : startsToContinue.values()) {
                            if (doEmit(start, true)) {
                                final Traverser.Admin<S> emitSplit = start.split();
                                emitSplit.resetLoops();
                                this.toReturn.add(IteratorUtils.of(emitSplit));
                            }
                            this.repeatTraversal.addStart(start);
                        }
                    } else {
                        //Place all starts on the repeatTraversal.
                        //As there is no until non of them are returned.
                        while (this.starts.hasNext()) {
                            foundSomething = true;
                            Traverser.Admin<S> start = starts.next();
                            this.repeatTraversal.addStart(start);
                            if (doEmit(start, true)) {
                                final Traverser.Admin<S> emitSplit = start.split();
                                emitSplit.resetLoops();
                                this.toReturn.add(IteratorUtils.of(emitSplit));
                            }
                        }
                    }
                    if (!foundSomething) {
                        throw FastNoSuchElementException.instance();
                    }
                } else {
                    while (this.starts.hasNext()) {
                        foundSomething = true;
                        Traverser.Admin<S> cachedStart = this.starts.next();
                        cachedStart.initialiseLoops(this.getId(), this.loopName);
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
                    if (!foundSomething) {
                        throw FastNoSuchElementException.instance();
                    }
                }
            }
        }
    }

    private boolean barrierUntil() {
        return this.untilTraversal != null && !this.untilTraversal.getSteps().isEmpty() && this.untilTraversal.getSteps().get(0) instanceof SqlgAbstractStep;
//        return TraversalHelper.anyStepRecursively((s) -> s instanceof SqlgAbstractStep, this.untilTraversal);
    }


    @Override
    protected Iterator<Traverser.Admin<S>> computerAlgorithm() throws NoSuchElementException {
        throw new IllegalStateException("computerAlgorithm not supported!");
    }

    protected class SqlgRepeatEndStepBarrier<S> extends SqlgComputerAwareStep<S, S> {

        private final List<Iterator<Traverser.Admin<S>>> toReturn;

        SqlgRepeatEndStepBarrier(final Traversal.Admin traversal) {
            super(traversal);
            this.toReturn = new ArrayList<>();
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Iterator<Traverser.Admin<S>> standardAlgorithm() throws NoSuchElementException {
            final SqlgRepeatStepBarrier<S> repeatStep = (SqlgRepeatStepBarrier<S>) this.getTraversal().getParent();
            while (true) {
                Iterator<Iterator<Traverser.Admin<S>>> returnIterator = toReturn.iterator();
                if (returnIterator.hasNext()) {
                    Iterator<Traverser.Admin<S>> next = returnIterator.next();
                    returnIterator.remove();
                    if (SqlgRepeatStepBarrier.this.sqlgRangeHolder != null && applyRange()) {
                        continue;
                    }
                    return next;
                }
                boolean foundSomething = false;
//                if (repeatStep.optimizeUntil) {
                if (barrierUntil()) {
                    if (!repeatStep.untilFirst) {
                        Multimap<String, Traverser.Admin<S>> startRecordIds = LinkedListMultimap.create();
                        foundSomething = repeatStep.doUntilBarrier(this.starts, this.toReturn, startRecordIds);
                        for (Traverser.Admin<S> start : startRecordIds.values()) {
                            //This is a brain bender.
                            if (!repeatStep.emitFirst) {
                                repeatStep.repeatTraversal.addStart(start);
                            } else {
                                //util is last and emit is first.
                                //the start here did not pass the until so it goes round again.
                                //The RepeatStep will iter the start and since emit is first it will emit it.
                                repeatStep.addStart(start);
                            }
                            if (repeatStep.doEmit(start, false)) {
                                final Traverser.Admin<S> emitSplit = start.split();
                                emitSplit.resetLoops();
                                this.toReturn.add(IteratorUtils.of(emitSplit));
                            }
                        }
                    } else {
                        //For untilFirst the starts are placed directly on the repeatStep.
                        //The end repeat step will return false and then the RepeatStep will check the untilTraversal before returning and/or continuing the repetition.
                        while (this.starts.hasNext()) {
                            foundSomething = true;
                            Traverser.Admin<S> start = starts.next();
                            start.initialiseLoops(SqlgRepeatStepBarrier.this.getId(), SqlgRepeatStepBarrier.this.loopName);
                            start.incrLoops();
                            repeatStep.addStart(start);
                            if (repeatStep.doEmit(start, false)) {
                                final Traverser.Admin<S> emitSplit = start.split();
                                emitSplit.resetLoops();
                                toReturn.add(IteratorUtils.of(emitSplit));
                            }
                        }
                    }
                    if (!foundSomething) {
                        throw FastNoSuchElementException.instance();
                    }
                } else {
                    while (this.starts.hasNext()) {
                        foundSomething = true;
                        Traverser.Admin<S> cachedStart = this.starts.next();
                        cachedStart.initialiseLoops(SqlgRepeatStepBarrier.this.getId(), SqlgRepeatStepBarrier.this.loopName);
                        cachedStart.incrLoops();
                        if (repeatStep.doUntil(cachedStart, false)) {
                            cachedStart.resetLoops();
                            toReturn.add(IteratorUtils.of(cachedStart));
                        } else {
                            if (!repeatStep.untilFirst && !repeatStep.emitFirst) {
                                repeatStep.repeatTraversal.addStart(cachedStart);
                            } else {
                                repeatStep.addStart(cachedStart);
                            }
                            if (repeatStep.doEmit(cachedStart, false)) {
                                final Traverser.Admin<S> emitSplit = cachedStart.split();
                                emitSplit.resetLoops();
                                toReturn.add(IteratorUtils.of(emitSplit));
                            }
                        }
                    }
                    if (!foundSomething) {
                        throw FastNoSuchElementException.instance();
                    }
                }
            }
        }

        @Override
        protected Iterator<Traverser.Admin<S>> computerAlgorithm() throws NoSuchElementException {
            throw new IllegalStateException("computerAlgorithm not supported!");
        }
    }

    public <S, E> boolean test(final Traverser.Admin<S> traverser, final Traversal.Admin<S, E> traversal) {
        if (this.untilHasContainer != null) {
            Element e = (Element) traverser.get();
            return this.untilHasContainer.getPredicate().test(e.value(this.untilHasContainer.getKey()));
        }
        if (traversal.getSteps().size() == 1 && traversal.getSteps().get(0) instanceof HasStep) {
            if (this.untilHasContainers == null) {
                HasStep hasStep = (HasStep) traversal.getSteps().get(0);
                //noinspection unchecked
                this.untilHasContainers = hasStep.getHasContainers();
                if (this.untilHasContainers.size() == 1) {
                    this.untilHasContainer = this.untilHasContainers.get(0);
                    Element e = (Element) traverser.get();
                    return this.untilHasContainer.getPredicate().test(e.value(this.untilHasContainer.getKey()));
                }
            }
            for (HasContainer hasContainer : this.untilHasContainers) {
                if (!hasContainer.test((Element) traverser.get())) {
                    return false;
                }
            }
            return true;
        } else {
            final Traverser.Admin<S> split = traverser.split();
            split.setSideEffects(traversal.getSideEffects());
            split.setBulk(1l);
            traversal.reset();
            traversal.addStart(split);
            return traversal.hasNext(); // filter
        }
    }

    private boolean applyRange() {
        if (this.sqlgRangeHolder.getRange().isBefore(this.rangeCount + 1)) {
            throw FastNoSuchElementException.instance();
        }
        if (this.sqlgRangeHolder.getRange().isAfter(this.rangeCount)) {
            this.rangeCount++;
            return true;
        }
        this.rangeCount++;
        return false;
    }

    private boolean doUntilBarrier(SqlgExpandableStepIterator<S> starts, List<Iterator<Traverser.Admin<S>>> toReturn, Multimap<String, Traverser.Admin<S>> startRecordIds) {
        boolean foundSomething = false;
        while (starts.hasNext()) {
            foundSomething = true;
            Traverser.Admin<S> cachedStart = starts.next();
            cachedStart.initialiseLoops(this.getId(), SqlgRepeatStepBarrier.this.loopName);
            cachedStart.incrLoops();
            List<Object> startObjects = cachedStart.path().objects();
            StringBuilder recordIdConcatenated = new StringBuilder();
            for (Object startObject : startObjects) {
                if (startObject instanceof Element) {
                    Element e = (Element) startObject;
                    recordIdConcatenated.append(e.id().toString());
                } else {
                    recordIdConcatenated.append(startObject.toString());
                }
            }
            startRecordIds.put(recordIdConcatenated.toString(), cachedStart);
            this.untilTraversal.addStart(cachedStart);
        }
        while (this.untilTraversal.hasNext()) {
            Traverser.Admin<?> filterTraverser = this.untilTraversal.nextTraverser();
            List<Object> filterTraverserObjects = filterTraverser.path().objects();
            String startId = "";
            for (Object filteredTraverserObject : filterTraverserObjects) {
                if (filteredTraverserObject instanceof Element) {
                    Element e = (Element) filteredTraverserObject;
                    startId += e.id().toString();
                } else {
                    startId += filteredTraverserObject.toString();
                }
                if (startRecordIds.containsKey(startId)) {
                    Collection<Traverser.Admin<S>> startsToReturn = startRecordIds.get(startId);
                    for (Traverser.Admin<S> start : startsToReturn) {
                        start.resetLoops();
                        toReturn.add(IteratorUtils.of(start));
                    }
                    startRecordIds.removeAll(startId);
                }
                if (startRecordIds.isEmpty()) {
                    break;
                }
            }
        }
        return foundSomething;
    }
}

