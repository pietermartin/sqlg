package org.umlg.sqlg.step.barrier;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.umlg.sqlg.step.SqlgAbstractStep;
import org.umlg.sqlg.structure.traverser.SqlgTraverser;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/09/28
 */
public class SqlgTraversalFilterStepBarrier<S> extends SqlgAbstractStep<S, S> implements TraversalParent {

    private boolean first = true;
    private final Traversal.Admin<S, ?> filterTraversal;
    private final List<Traverser.Admin<S>> results = new ArrayList<>();
    private Iterator<Traverser.Admin<S>> resultIterator;

    public SqlgTraversalFilterStepBarrier(Traversal.Admin traversal, Traversal.Admin<S, ?> filterTraversal) {
        super(traversal);
        this.filterTraversal = this.integrateChild(filterTraversal.asAdmin());
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        if (this.first) {
            this.first = false;
            Multimap<String, Traverser.Admin<S>> startRecordIds = LinkedListMultimap.create();
            while (this.starts.hasNext()) {
                Traverser.Admin<S> start = this.starts.next();
                //Well now, can't say I really really get this.
                //The TinkerPop's TraversalFilterStep resets the traversal for each start.
                //This in turn clears TraverserSet's internal map. So the result for every start is added to an empty map. This means
                //no merging happens which means no bulking happens. Look at TraverserSet.add
                //This requirement says no bulking must happen.
                //If the strategy finds TraverserRequirement.ONE_BULK it will set requiredOneBulk = true on the traverser.
                //However the traverser might have been created from a step and strategy that does not care for TraverserRequirement.ONE_BULK so we force it here.
                if (start instanceof SqlgTraverser) {
                    ((SqlgTraverser) start).setRequiresOneBulk(true);
                }
                List<Object> startObjects = start.path().objects();
                StringBuilder recordIdConcatenated = new StringBuilder();
                for (Object startObject : startObjects) {
                    if (startObject instanceof Element) {
                        Element e = (Element) startObject;
                        recordIdConcatenated.append(e.id().toString());
                    } else {
                        recordIdConcatenated.append(startObject.toString());
                    }
                }
                startRecordIds.put(recordIdConcatenated.toString(), start);

                this.filterTraversal.addStart(start);
            }
            while (this.filterTraversal.hasNext()) {
                Traverser.Admin<?> filterTraverser = this.filterTraversal.nextTraverser();
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
                        results.addAll(startRecordIds.get(startId));
                        startRecordIds.removeAll(startId);
                    }
                    if (startRecordIds.isEmpty()) {
                        break;
                    }
                }

            }
            this.results.sort((o1, o2) -> {
                SqlgTraverser x = (SqlgTraverser) o1;
                SqlgTraverser y = (SqlgTraverser) o2;
                return Long.compare(x.getStartElementIndex(), y.getStartElementIndex());
            });
            this.resultIterator = this.results.iterator();
        }
        if (this.resultIterator.hasNext()) {
            return this.resultIterator.next();
        } else {
            //The standard TraversalFilterStep.filter calls TraversalUtil.test which normally resets the traversal for every incoming start.
            reset();
            throw FastNoSuchElementException.instance();
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.first = true;
        this.results.clear();
        this.resultIterator = null;
        this.filterTraversal.reset();
        this.getLocalChildren().forEach(Traversal.Admin::reset);
        this.getGlobalChildren().forEach(Traversal.Admin::reset);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Traversal.Admin<S, ?>> getLocalChildren() {
        return Collections.singletonList(this.filterTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.ONE_BULK);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.filterTraversal);
    }
}
