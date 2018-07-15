package org.umlg.sqlg.step.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.umlg.sqlg.step.SqlgAbstractStep;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.structure.SqlgTraverser;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/04/24
 */
public abstract class SqlgBranchStepBarrier<S, E, M> extends SqlgAbstractStep<S, E> implements TraversalOptionParent<M, S, E> {

    private Traversal.Admin<S, M> branchTraversal;
    Map<M, List<Traversal.Admin<S, E>>> traversalOptions = new HashMap<>();
    private boolean first = true;
    private final List<Traverser.Admin<E>> results = new ArrayList<>();
    private Iterator<Traverser.Admin<E>> resultIterator;

    SqlgBranchStepBarrier(final Traversal.Admin traversal) {
        super(traversal);
    }

    void setBranchTraversal(final Traversal.Admin<S, M> branchTraversal) {
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

        if (this.first) {

            List<Traverser.Admin<S>> successfulStarts = new ArrayList<>();
            Map<Long, Traverser.Admin<S>> cachedStarts = new HashMap<>();
            Map<Traverser.Admin<S>, Object> startBranchTraversalResults = new HashMap<>();

            this.first = false;

            long startCount = 1;
            while (this.starts.hasNext()) {
                Traverser.Admin<S> start = this.starts.next();
                this.branchTraversal.addStart(start);
                ((SqlgElement) start.get()).setInternalStartTraverserIndex(startCount);
                cachedStarts.put(startCount++, start);
            }

            Set<Long> toRemove = new HashSet<>();
            while (true) {
                if (this.branchTraversal.hasNext()) {
                    Traverser.Admin<M> branchTraverser = this.branchTraversal.nextTraverser();
                    long startElementIndex = ((SqlgTraverser<M>) branchTraverser).getStartElementIndex();
                    M m = branchTraverser.get();
                    //startElementIndex is 0 for branchTraversals that do not contain a SqlgVertexStep.
                    //i.e. traversals that do not go to the db.
                    if (startElementIndex == 0 && m instanceof SqlgElement) {
                        SqlgElement sqlgElement = (SqlgElement) m;
                        startElementIndex = sqlgElement.getInternalStartTraverserIndex();
                    }
                    if (!(m instanceof SqlgElement)) {
                        //This assumes that branchTraversals that do not go to the db only return one value per start.
                        List<Object> branchTraverserPathObjects = branchTraverser.path().objects();
                        long count = 0;
                        for (Traverser.Admin<S> cachedStart : cachedStarts.values()) {
                            count++;
                            List<Object> cachedStartPathObjects = cachedStart.path().objects();
                            boolean startsWith = false;
                            //for CountGlobalStep the path is lost but all elements return something so the branch to take is always the 'true' branch.
                            if (!(branchTraverserPathObjects.get(0) instanceof SqlgElement)) {
                                startsWith = true;
                            }
                            if (!startsWith) {
                                int countX = 0;
                                for (Object startObject : cachedStartPathObjects) {
                                    startsWith = branchTraverserPathObjects.get(countX++).equals(startObject);
                                    if (!startsWith) {
                                        break;
                                    }
                                }
                            }
                            if (startsWith) {
                                successfulStarts.add(cachedStart);
                                toRemove.add(count);
                                startBranchTraversalResults.put(cachedStart, branchTraverserPathObjects.get(branchTraverserPathObjects.size() - 1));
                            }
                        }
                    } else {
                        Traverser.Admin<S> start = cachedStarts.remove(startElementIndex);
                        if (start != null) {
                            successfulStarts.add(start);
                        }
                    }
                } else {
                    break;
                }
            }
            //remove all successful starts from the cachedStarts.
            //i.e. only failed starts remain in the list.
            for (Long remove : toRemove) {
                cachedStarts.remove(remove);
            }

            //true false choose step does not have a HasNextStep. Its been removed to keep the path.
            if (this.traversalOptions.containsKey(Boolean.TRUE) && this.traversalOptions.containsKey(Boolean.FALSE)) {
                for (Traverser.Admin<S> successfulStart : successfulStarts) {
                    for (Traversal.Admin<S, E> optionTraversal : this.traversalOptions.get(Boolean.TRUE)) {
                        optionTraversal.addStart(successfulStart);
                    }
                }
                for (Traversal.Admin<S, E> optionTraversal : this.traversalOptions.get(Boolean.FALSE)) {
                    for (Traverser.Admin<S> start : cachedStarts.values()) {
                        optionTraversal.addStart(start);
                    }
                }
            } else {
                for (Map.Entry<Traverser.Admin<S>, Object> entry : startBranchTraversalResults.entrySet()) {
                    Traverser.Admin<S> start = entry.getKey();
                    Object branchEndObject = entry.getValue();
                    if (this.traversalOptions.containsKey(branchEndObject)) {
                        for (Traversal.Admin<S, E> optionTraversal : this.traversalOptions.get(branchEndObject)) {
                            optionTraversal.addStart(start);
                        }
                    } else {
                        if (this.traversalOptions.containsKey(Pick.none)) {
                            for (Traversal.Admin<S, E> optionTraversal : this.traversalOptions.get(Pick.none)) {
                                optionTraversal.addStart(start);
                            }
                        }
                    }
                }
//                List<Traversal.Admin<S, E>> optionTraversals = this.traversalOptions.get(Pick.none);
//                for (Traversal.Admin<S, E> optionTraversal : optionTraversals) {
//                    for (Traverser.Admin<S> start : cachedStarts.values()) {
//                        optionTraversal.addStart(start);
//                    }
//                }
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
            this.results.sort((o1, o2) -> {
                SqlgTraverser x = (SqlgTraverser) o1;
                SqlgTraverser y = (SqlgTraverser) o2;
                return Long.compare(x.getStartElementIndex(), y.getStartElementIndex());
            });
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

    @SuppressWarnings("unchecked")
    @Override
    public List<Traversal.Admin<S, E>> getGlobalChildren() {
        return Collections.unmodifiableList(this.traversalOptions.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Traversal.Admin<S, M>> getLocalChildren() {
        return Collections.singletonList(this.branchTraversal);
    }

}
