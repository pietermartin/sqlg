package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.umlg.sqlg.strategy.SqlgNoSuchElementException;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/04/24
 */
public class SqlgBranchStep<S, E, M> extends AbstractStep<S, E> implements TraversalOptionParent<M, S, E> {

    protected Traversal.Admin<S, M> branchTraversal;
    protected Map<M, List<Traversal.Admin<S, E>>> traversalOptions = new HashMap<>();
    private boolean first = true;

    public SqlgBranchStep(final Traversal.Admin traversal) {
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
        while (this.starts.hasNext()) {
            Traverser.Admin<S> start = this.starts.next();
            this.branchTraversal.addStart(start);
            tmpStarts.add(start);
        }

        if (this.first) {
            this.first = false;
            while (this.branchTraversal.hasNext()) {
                Traverser.Admin<M> branchTraverser = this.branchTraversal.nextTraverser();
                for (Traverser.Admin<S> tmpStart : tmpStarts) {
                    List<Object> startObjects = tmpStart.path().objects();
                    List<Object> optionObjects = branchTraverser.path().objects();
                    int count = 0;
                    boolean startsWith = true;
                    for (Object startObject : startObjects) {
                        startsWith = optionObjects.get(count++).equals(startObject);
                        if (!startsWith) {
                            break;
                        }
                    }
                    if (startsWith) {
                        for (Traversal.Admin<S, E> trueOption : this.traversalOptions.get(true)) {
                            trueOption.addStart(tmpStart);
                            tmpStartsToRemove.add(tmpStart);
                        }
                    }
                }
            }
            tmpStarts.removeAll(tmpStartsToRemove);
            for (Traverser.Admin<S> tmpStart : tmpStarts) {
                for (Traversal.Admin<S, E> falseOption : this.traversalOptions.get(false)) {
                    falseOption.addStart(tmpStart);
                }
            }

        }
        try {
            for (Traversal.Admin<S, E> option : this.traversalOptions.get(true)) {
                if (option.hasNext()) {
                    return option.nextTraverser();
                }
            }
        } catch (SqlgNoSuchElementException e) {
            //swallow
        }
        for (Traversal.Admin<S, E> option : this.traversalOptions.get(false)) {
            if (option.hasNext()) {
                return option.nextTraverser();
            }
        }
        throw new SqlgNoSuchElementException();
    }

//    @Override
//    protected Iterator<Traverser.Admin<E>> standardAlgorithm() {
//        while (true) {
//            if (!this.first) {
//                for (final List<Traversal.Admin<S, E>> options : this.traversalOptions.values()) {
//                    for (final Traversal.Admin<S, E> option : options) {
//                        if (option.hasNext())
//                            return option.getEndStep();
//                    }
//                }
//            }
//            this.first = false;
//            ///
////            if (this.hasBarrier) {
//            if (!this.starts.hasNext())
//                throw FastNoSuchElementException.instance();
//
//            List<Traverser.Admin<S>> tmpStarts = new ArrayList<>();
//            while (this.starts.hasNext()) {
//                Traverser.Admin<S> start = this.starts.next();
//                this.branchTraversal.addStart(start);
//                tmpStarts.add(start);
////                this.handleStart(start);
//            }
//
//            List<Traversal.Admin<S, E>> tmpOptions = new ArrayList<>();
//            while (this.branchTraversal.hasNext()) {
//                Traverser.Admin<M> branchTraverser = this.branchTraversal.nextTraverser();
//                M branch = branchTraverser.get();
//                Traversal.Admin<S, E> option = (Traversal.Admin<S, E>) this.traversalOptions.get(branch);
//                for (Traverser.Admin<S> tmpStart : tmpStarts) {
//                    option.addStart(tmpStart);
//                }
//                tmpOptions.add(option);
//            }
//
//            for (Traversal.Admin<S, E> tmpOption : tmpOptions) {
//                while (tmpOption.hasNext()) {
//                    return tmpOption.nextTraverser();
//                }
//            }
//
////            while (true) {
////                try {
////                    if (this.branchTraversal.hasNext()) {
////                        Traverser.Admin<M> traverser = this.branchTraversal.nextTraverser();
////                        return traverser;
////                    }
////                } catch (SqlgResetTraversalException e) {
////                    this.branchTraversal.reset();
////                } catch (SqlgNoSuchElementException e) {
////                    e.printStackTrace();
////                    throw FastNoSuchElementException.instance();
////                }
////            }
//
//
////            } else {
////                this.handleStart(this.starts.next());
////            }
//        }
//    }

    private final void handleStart(final Traverser.Admin<S> start) {
        final M choice = TraversalUtil.apply(start, this.branchTraversal);
        final List<Traversal.Admin<S, E>> branch = this.traversalOptions.containsKey(choice) ? this.traversalOptions.get(choice) : this.traversalOptions.get(Pick.none);
        if (null != branch)
            branch.forEach(traversal -> traversal.addStart(start.split()));
        if (choice != Pick.any) {
            final List<Traversal.Admin<S, E>> anyBranch = this.traversalOptions.get(Pick.any);
            if (null != anyBranch)
                anyBranch.forEach(traversal -> traversal.addStart(start.split()));
        }
    }

    @Override
    public SqlgBranchStep<S, E, M> clone() {
        final SqlgBranchStep<S, E, M> clone = (SqlgBranchStep<S, E, M>) super.clone();
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
