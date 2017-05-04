package org.umlg.sqlg.step;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.structure.SqlgTraverser;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/04/24
 */
public class SqlgChooseStepBarrier<S, E, M> extends SqlgBranchStepBarrier<S, E, M> {

    private boolean isOptionalStep = false;

    public SqlgChooseStepBarrier(final Traversal.Admin traversal, final Traversal.Admin<S, M> choiceTraversal, boolean isOptionalStep) {
        super(traversal);
        this.setBranchTraversal(choiceTraversal);
        this.isOptionalStep = isOptionalStep;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        if (!this.isOptionalStep) {
            return super.processNextStart();
        } else {
            if (this.first) {
                List<Traverser.Admin<S>> tmpStarts = new ArrayList<>();
                List<Traverser.Admin<S>> tmpStartsToRemove = new ArrayList<>();
                Map<M, List<Traverser.Admin<S>>> predicateWithSuccessfulStarts = new HashMap<>();
                this.first = false;
                long startCount = 1;

                while (this.starts.hasNext()) {
                    Traverser.Admin<S> start = this.starts.next();
                    this.branchTraversal.addStart(start);
                    ((SqlgElement) start.get()).setInternalStartTraverserIndex(startCount++);
                    tmpStarts.add(start);
                }
                while (true) {
                    if (this.branchTraversal.hasNext()) {
                        Traverser.Admin<M> branchTraverser = this.branchTraversal.nextTraverser();
                        this.results.add((Traverser.Admin<E>) branchTraverser);
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

                Preconditions.checkState(this.traversalOptions.containsKey(Boolean.TRUE) && this.traversalOptions.containsKey(Boolean.FALSE),
                        "ChooseStep's traversalOptions must contain true and false as keys!");
                Preconditions.checkState(this.traversalOptions.get(Boolean.FALSE).size() == 1);
                Preconditions.checkState(this.traversalOptions.get(Boolean.FALSE).get(0).getSteps().stream().anyMatch(s -> s instanceof IdentityStep));

                //true false choose step does not have a HasNextStep. Its been removed to keep the path.
                if (this.traversalOptions.containsKey(Boolean.TRUE) && this.traversalOptions.containsKey(Boolean.FALSE)) {
                    //Add the unsuccessful entries to the identity traversal
                    List<Traversal.Admin<S, E>> optionTraversals = this.traversalOptions.get(Boolean.FALSE);
                    for (Traversal.Admin<S, E> optionTraversal : optionTraversals) {
                        for (Traverser.Admin<S> start : tmpStarts) {
                            optionTraversal.addStart(start);
                            //Bulking logic interferes here, addStart calls DefaultTraversal.merge which has bulking logic
                            start.setBulk(1L);
                        }
                    }
                }
                //Now travers the options. The starts have been set.
                for (Traversal.Admin<S, E> option : this.traversalOptions.get(Boolean.FALSE)) {
                    while (true) {
                        if (option.hasNext()) {
                            this.results.add(option.nextTraverser());
                        } else {
                            break;
                        }
                    }
                }
                //Sort the results, this is to ensure the the incoming start order is not lost.
                this.results.sort((o1, o2) -> {
                    SqlgTraverser x = (SqlgTraverser) o1;
                    SqlgTraverser y = (SqlgTraverser) o1;
                    return (x.getStartElementIndex() < y.getStartElementIndex()) ? -1 : ((x.getStartElementIndex() == y.getStartElementIndex()) ? 0 : 1);
                });
                this.resultIterator = this.results.iterator();
            }
            while (this.resultIterator.hasNext()) {
                return this.resultIterator.next();
            }
            throw FastNoSuchElementException.instance();
        }
    }

    public Map<M, List<Traversal.Admin<S, E>>> getTraversalOptions() {
        return this.traversalOptions;
    }

    @Override
    public void addGlobalChildOption(final M pickToken, final Traversal.Admin<S, E> traversalOption) {
        if (Pick.any.equals(pickToken))
            throw new IllegalArgumentException("Choose step can not have an any-option as only one option per traverser is allowed");
        if (this.traversalOptions.containsKey(pickToken))
            throw new IllegalArgumentException("Choose step can only have one traversal per pick token: " + pickToken);
        super.addGlobalChildOption(pickToken, traversalOption);
    }

    public boolean isOptionalStep() {
        return isOptionalStep;
    }
}
