package org.umlg.sqlg.step;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.structure.SqlgTraverser;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/09/28
 */
public class SqlgTraversalFilterStepBarrier<S> extends AbstractStep<S, S> implements TraversalParent {

    private boolean first = true;
    private Traversal.Admin<S, ?> filterTraversal;
    private List<Traverser.Admin<S>> results = new ArrayList<>();
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
                SqlgElement sqlgElement = (SqlgElement) start.get();
                startRecordIds.put(sqlgElement.id().toString(), start);
                this.filterTraversal.addStart(start);
            }
            Set<String> toRemainRecordIds = new HashSet<>();
            while (this.filterTraversal.hasNext()) {
                Traverser.Admin<?> filterTraverser = this.filterTraversal.nextTraverser();
                List<Object> filterTraverserObjects = filterTraverser.path().objects();
                //Take the first as its the incoming.
                SqlgElement incoming = (SqlgElement)filterTraverserObjects.get(0);
                toRemainRecordIds.add(incoming.id().toString());
            }
            for (Map.Entry<String, Traverser.Admin<S>> startEntry : startRecordIds.entries()) {
                String key = startEntry.getKey();
                Traverser.Admin<S> value = startEntry.getValue();
                if (toRemainRecordIds.contains(key)) {
                    this.results.add(value);
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
            Traverser.Admin<S> traverser = this.resultIterator.next();
            return traverser;
        } else {
            throw FastNoSuchElementException.instance();
        }
    }
}
