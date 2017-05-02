package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.structure.SqlgTraverser;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/04/24
 */
public class SqlgBranchStepBarrier<S, E, M> extends AbstractStep<S, E> implements TraversalOptionParent<M, S, E> {

    protected Traversal.Admin<S, M> branchTraversal;
    protected Map<M, List<Traversal.Admin<S, E>>> traversalOptions = new HashMap<>();
    private boolean first = true;
    private List<Traverser.Admin<E>> results = new ArrayList<>();
    private Iterator<Traverser.Admin<E>> resultIterator;

    public SqlgBranchStepBarrier(final Traversal.Admin traversal) {
        super(traversal);
    }

    public void setBranchTraversal(final Traversal.Admin<S, M> branchTraversal) {
        this.branchTraversal = this.integrateChild(branchTraversal);
    }

    @Override
    public void addGlobalChildOption(final M pickToken, final Traversal.Admin<S, E> traversalOption) {
        if (this.traversalOptions.containsKey(pickToken))
            this.traversalOptions.get(pickToken).add(traversalOption);
        else
            this.traversalOptions.put(pickToken, new ArrayList<>(Collections.singletonList(traversalOption)));
        this.integrateChild(traversalOption);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        List<Traverser.Admin<S>> tmpStarts = new ArrayList<>();
        List<Traverser.Admin<S>> tmpStartsToRemove = new ArrayList<>();
        Map<M, List<Traverser.Admin<S>>> predicateWithSuccessfulStarts = new HashMap<>();

        if (this.first) {

            this.first = false;

            long startCount = 1;
            while (this.starts.hasNext()) {
                Traverser.Admin<S> start = this.starts.next();
                this.branchTraversal.addStart(start);
                ((SqlgElement)start.get()).setInternalStartTraverserIndex(startCount++);
                tmpStarts.add(start);
            }

            while (true) {
                if (this.branchTraversal.hasNext()) {
                    Traverser.Admin<M> branchTraverser = this.branchTraversal.nextTraverser();

                    for (Traverser.Admin<S> tmpStart : tmpStarts) {
                        List<Object> startObjects = tmpStart.path().objects();
                        List<Object> optionObjects = branchTraverser.path().objects();
                        int count = 0;
                        boolean startsWith = false;
                        //for CountGlobalStep the path is lost but all elements return something so the branch to take is always the 'true' branch.
                        if (!(optionObjects.get(0) instanceof SqlgElement)) {
                            startsWith = true;
                        }
                        if (!startsWith) {
                            for (Object startObject : startObjects) {
                                startsWith = optionObjects.get(count++).equals(startObject);
                                if (!startsWith) {
                                    break;
                                }
                            }
                        }
                        if (startsWith) {
                            List<Traverser.Admin<S>> starts = predicateWithSuccessfulStarts.computeIfAbsent(branchTraverser.get(), k -> new ArrayList<>());
                            starts.add(tmpStart);
                            tmpStartsToRemove.add(tmpStart);
                        }
                    }
                } else {
                    break;
                }
            }
            tmpStarts.removeAll(tmpStartsToRemove);

            //true false choose step does not have a HasNextStep. Its been removed to keep the path.
            if (this.traversalOptions.containsKey(Boolean.TRUE) && this.traversalOptions.containsKey(Boolean.FALSE)) {
                for (Map.Entry<M, List<Traverser.Admin<S>>> entry : predicateWithSuccessfulStarts.entrySet()) {
                    for (Traverser.Admin<S> start : entry.getValue()) {
                        for (Traversal.Admin<S, E> optionTraversal : this.traversalOptions.get(Boolean.TRUE)) {
                            optionTraversal.addStart(start);
                            //Bulking logic interferes here, addStart calls DefaultTraversal.merge which has bulking logic
                            start.setBulk(1L);
                        }
                    }
                }
                List<Traversal.Admin<S, E>> optionTraversals = this.traversalOptions.get(Boolean.FALSE);
                for (Traversal.Admin<S, E> optionTraversal : optionTraversals) {
                    for (Traverser.Admin<S> start : tmpStarts) {
                        optionTraversal.addStart(start);
                        //Bulking logic interferes here, addStart calls DefaultTraversal.merge which has bulking logic
                        start.setBulk(1L);
                    }
                }
            } else {
                for (Map.Entry<M, List<Traverser.Admin<S>>> entry : predicateWithSuccessfulStarts.entrySet()) {
                    for (Traverser.Admin<S> start : entry.getValue()) {
                        if (this.traversalOptions.containsKey(entry.getKey())) {
                            for (Traversal.Admin<S, E> optionTraversal : this.traversalOptions.get(entry.getKey())) {
                                optionTraversal.addStart(start);
                                //Bulking logic interferes here, addStart calls DefaultTraversal.merge which has bulking logic
                                start.setBulk(1L);
                            }
                        } else {
                            if (this.traversalOptions.containsKey(Pick.none)) {
                                for (Traversal.Admin<S, E> optionTraversal : this.traversalOptions.get(Pick.none)) {
                                    optionTraversal.addStart(start);
                                    //Bulking logic interferes here, addStart calls DefaultTraversal.merge which has bulking logic
                                    start.setBulk(1L);
                                }
                            }
                        }
                    }
                }
                List<Traversal.Admin<S, E>> optionTraversals = this.traversalOptions.get(Pick.none);
                for (Traversal.Admin<S, E> optionTraversal : optionTraversals) {
                    for (Traverser.Admin<S> start : tmpStarts) {
                        optionTraversal.addStart(start);
                        //Bulking logic interferes here, addStart calls DefaultTraversal.merge which has bulking logic
                        start.setBulk(1L);
                    }
                }
            }
            //Now travers the options. The starts have been set.
            for (M choice : this.traversalOptions.keySet()) {
                for (Traversal.Admin<S, E> option : this.traversalOptions.get(choice)) {
                    while (true) {
                        if (option.hasNext()) {
                            this.results.add(option.nextTraverser());
                        } else {
                            break;
                        }
                    }
                }
            }
            //Sort the results, this is to ensure the the incoming start order is not lost.
            Collections.sort(this.results, (o1, o2) -> {
                SqlgTraverser x = (SqlgTraverser)o1;
                SqlgTraverser y = (SqlgTraverser)o1;
                return (x.getStartElementIndex() < y.getStartElementIndex()) ? -1 : ((x.getStartElementIndex() == y.getStartElementIndex()) ? 0 : 1);
            });
//            Collections.sort(results, (o1, o2) -> {
//                long x = ((SqlgElement)o1.get()).getInternalStartTraverserIndex();
//                long y = ((SqlgElement)o2.get()).getInternalStartTraverserIndex();
//                return (x < y) ? -1 : ((x == y) ? 0 : 1);
//            });
            this.resultIterator = this.results.iterator();
        }
        while (this.resultIterator.hasNext()) {
            return this.resultIterator.next();
        }
        throw FastNoSuchElementException.instance();
    }

    @Override
    public SqlgBranchStepBarrier<S, E, M> clone() {
        final SqlgBranchStepBarrier<S, E, M> clone = (SqlgBranchStepBarrier<S, E, M>) super.clone();
        clone.traversalOptions = new HashMap<>(this.traversalOptions.size());
        for (final Map.Entry<M, List<Traversal.Admin<S, E>>> entry : this.traversalOptions.entrySet()) {
            final List<Traversal.Admin<S, E>> traversals = entry.getValue();
            if (traversals.size() > 0) {
                final List<Traversal.Admin<S, E>> clonedTraversals = clone.traversalOptions.compute(entry.getKey(), (k, v) ->
                        (v == null) ? new ArrayList<>(traversals.size()) : v);
                for (final Traversal.Admin<S, E> traversal : traversals) {
                    clonedTraversals.add(traversal.clone());
                }
            }
        }
        clone.branchTraversal = this.branchTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.branchTraversal);
        this.traversalOptions.values().stream().flatMap(List::stream).forEach(this::integrateChild);
    }


    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (this.traversalOptions != null)
            result ^= this.traversalOptions.hashCode();
        if (this.branchTraversal != null)
            result ^= this.branchTraversal.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.branchTraversal, this.traversalOptions);
    }

    @Override
    public void reset() {
        super.reset();
        this.first = true;
        this.results.clear();
        this.getLocalChildren().forEach(Traversal.Admin::reset);
        this.getGlobalChildren().forEach(Traversal.Admin::reset);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    @Override
    public List<Traversal.Admin<S, E>> getGlobalChildren() {
        return Collections.unmodifiableList(this.traversalOptions.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList()));
    }

    @Override
    public List<Traversal.Admin<S, M>> getLocalChildren() {
        return Collections.singletonList(this.branchTraversal);
    }

}
