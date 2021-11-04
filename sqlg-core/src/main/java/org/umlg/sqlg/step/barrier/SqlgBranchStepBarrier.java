package org.umlg.sqlg.step.barrier;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.PredicateTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;
import org.umlg.sqlg.step.SqlgAbstractStep;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.structure.traverser.SqlgTraverser;
import org.umlg.sqlg.util.SqlgTraversalUtil;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/04/24
 */
@SuppressWarnings("rawtypes")
public abstract class SqlgBranchStepBarrier<S, E, M> extends SqlgAbstractStep<S, E> implements TraversalOptionParent<M, S, E> {

    protected Traversal.Admin<S, M> branchTraversal;
    protected Map<Pick, List<Traversal.Admin<S, E>>> traversalPickOptions = new HashMap<>();
    protected List<Pair<Traversal.Admin, Traversal.Admin<S, E>>> traversalOptions = new ArrayList<>();


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

        if (pickToken instanceof Pick) {
            if (this.traversalPickOptions.containsKey(pickToken))
                this.traversalPickOptions.get(pickToken).add(traversalOption);
            else
                this.traversalPickOptions.put((Pick) pickToken, new ArrayList<>(Collections.singletonList(traversalOption)));
        } else {
            final Traversal.Admin pickOptionTraversal;
            if (pickToken instanceof Traversal) {
                pickOptionTraversal = ((Traversal) pickToken).asAdmin();
            } else {
                pickOptionTraversal = new PredicateTraversal(pickToken);
            }
            this.traversalOptions.add(Pair.with(pickOptionTraversal, traversalOption));
        }
        this.integrateChild(traversalOption);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {

        if (this.first) {

            Map<Long, Traverser.Admin<S>> cachedStarts = new HashMap<>();
            Map<Traverser.Admin<S>, Object> startBranchTraversalResults = new HashMap<>();

            this.first = false;

            long startCount = 1;
            while (this.starts.hasNext()) {
                Traverser.Admin<S> start = this.starts.next();
                this.branchTraversal.addStart(start);
                if (start.get() instanceof SqlgElement) {
                    ((SqlgElement) start.get()).setInternalStartTraverserIndex(startCount);
                }
                cachedStarts.put(startCount++, start);
            }
            while (true) {
                //Branch traversal assumes every start only returns exactly one result.
                //If it returns more than one then first value is taken.
                //This is taken from Tinkerpop's branch traversal's logic.
                //BranchStep.applyCurrentTraverser
                //final Object choice = TraversalUtil.apply(start, this.branchTraversal);
                if (!cachedStarts.isEmpty() && this.branchTraversal.hasNext()) {
                    Traverser.Admin<M> branchTraverser = this.branchTraversal.nextTraverser();
                    M m = branchTraverser.get();
                    //startElementIndex is 0 for branchTraversals that do not contain a SqlgVertexStep.
                    //This assumes that branchTraversals that do not go to the db only return one value per start.
                    List<Object> branchTraverserPathObjects = branchTraverser.path().objects();
                    for (Map.Entry<Long, Traverser.Admin<S>> cachedStartEntry : cachedStarts.entrySet()) {
                        startCount = cachedStartEntry.getKey();
                        Traverser.Admin<S> cachedStart = cachedStartEntry.getValue();
                        if (cachedStart.get() instanceof SqlgElement) {
                            startCount = ((SqlgElement) cachedStart.get()).getInternalStartTraverserIndex();
                        }
                        boolean startsWith = m instanceof Boolean && (!((Boolean) m)) || !(branchTraverserPathObjects.get(0) instanceof SqlgElement);
                        //for CountGlobalStep the path is lost but all elements return something so the branch to take is always the 'true' branch.
                        //for SqlgHasNextStep the path is lost on 'false'.
                        if (!startsWith) {
                            Object cachedStartObject = cachedStart.get();
                            long internalStartTraversalIndex = ((SqlgElement) cachedStartObject).getInternalStartTraverserIndex();
                            for (int i = branchTraverserPathObjects.size() - 1; i >= 0; i--) {
                                Object branchTraversalPathObject = branchTraverserPathObjects.get(i);
                                if (branchTraversalPathObject instanceof SqlgElement &&
                                        ((SqlgElement) branchTraversalPathObject).getInternalStartTraverserIndex() == internalStartTraversalIndex &&
                                        branchTraversalPathObject.equals(cachedStartObject)) {
                                    startsWith = true;
                                    break;
                                }
                            }
                        }
                        if (startsWith) {
                            Traverser.Admin<S> start = cachedStarts.remove(startCount);
                            startBranchTraversalResults.put(start, branchTraverserPathObjects.get(branchTraverserPathObjects.size() - 1));
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
            Preconditions.checkState(cachedStarts.isEmpty());
            for (Traverser.Admin<S> successfulStart : startBranchTraversalResults.keySet()) {
                Object branchEndObject = startBranchTraversalResults.get(successfulStart);
                List<Traversal.Admin<S, E>> branches = pickBranches(branchEndObject);
                for (Traversal.Admin<S, E> branch : branches) {
                    branch.addStart(successfulStart);
                }
            }

            for (Pair<Traversal.Admin, Traversal.Admin<S, E>> traversalOptionPair : traversalOptions) {
                Traversal.Admin<S, E> traversalOption = traversalOptionPair.getValue1();
                while (true) {
                    //hasStarts do not work as Sqlg frequently barriers all the starts upfront in which case hasStarts will return false on the second iteration.
                    if (traversalOption.getStartStep().hasStarts() && traversalOption.hasNext()) {
//                    if (traversalOption.getStartStep().hasNext() && traversalOption.hasNext()) {
                        this.results.add(traversalOption.nextTraverser());
                    } else {
                        break;
                    }
                }
            }
            for (Map.Entry<Pick, List<Traversal.Admin<S, E>>> traversalPickOptionsEntry : traversalPickOptions.entrySet()) {
                Pick pick = traversalPickOptionsEntry.getKey();
                List<Traversal.Admin<S, E>> pickTraversals = traversalPickOptionsEntry.getValue();
                for (Traversal.Admin<S, E> pickTraversal : pickTraversals) {
                    while (true) {
                        if (pickTraversal.hasNext()) {
                            this.results.add(pickTraversal.nextTraverser());
                        } else {
                            break;
                        }
                    }
                }
            }

            //Sort the results, this is to ensure the incoming start order is not lost.
            this.results.sort((o1, o2) -> {
                SqlgTraverser x = (SqlgTraverser) o1;
                SqlgTraverser y = (SqlgTraverser) o2;
                return Long.compare(x.getStartElementIndex(), y.getStartElementIndex());
            });
            this.resultIterator = this.results.iterator();
        }
        //noinspection LoopStatementThatDoesntLoop
        while (this.resultIterator.hasNext()) {
            return this.resultIterator.next();
        }
        throw FastNoSuchElementException.instance();
    }

    private List<Traversal.Admin<S, E>> pickBranches(final Object choice) {
        final List<Traversal.Admin<S, E>> branches = new ArrayList<>();
        if (choice instanceof Pick) {
            if (this.traversalPickOptions.containsKey(choice)) {
                branches.addAll(this.traversalPickOptions.get(choice));
            }
        }
        for (final Pair<Traversal.Admin, Traversal.Admin<S, E>> p : this.traversalOptions) {
            if (SqlgTraversalUtil.test(choice, p.getValue0())) {
                branches.add(p.getValue1());
            }
        }
        return branches.isEmpty() ? this.traversalPickOptions.get(Pick.none) : branches;
    }

    @Override
    public SqlgBranchStepBarrier<S, E, M> clone() {
        return (SqlgBranchStepBarrier<S, E, M>) super.clone();
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.branchTraversal);
        this.getGlobalChildren().forEach(this::integrateChild);
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
        return Collections.unmodifiableList(Stream.concat(
                this.traversalPickOptions.values().stream().flatMap(List::stream),
                this.traversalOptions.stream().map(Pair::getValue1)).collect(Collectors.toList()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Traversal.Admin<S, M>> getLocalChildren() {
        return Collections.singletonList(this.branchTraversal);
    }

}
