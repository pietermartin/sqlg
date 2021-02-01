package org.umlg.sqlg.step.barrier;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.umlg.sqlg.step.SqlgConnectiveStep;
import org.umlg.sqlg.structure.traverser.SqlgTraverser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/10/26
 */
public class SqlgOrStepBarrier<S> extends SqlgConnectiveStep<S> {

    private boolean first = true;
    private List<Traverser.Admin<S>> results = new ArrayList<>();
    private Iterator<Traverser.Admin<S>> resultIterator;
    private final List<Traversal.Admin<S, ?>> orTraversals = new ArrayList<>();

    public SqlgOrStepBarrier(final Traversal.Admin traversal, final Collection<Traversal<S, ?>> traversals) {
        super(traversal, traversals);
        for (Traversal<S, ?> sTraversal : traversals) {
            //noinspection unchecked
            this.orTraversals.add((Traversal.Admin<S, ?>) sTraversal);
        }
    }

    @Override
    protected Traverser.Admin<S> processNextStart() {
        if (this.first) {
            this.first = false;
            Multimap<String, Traverser.Admin<S>> startRecordIds = LinkedListMultimap.create();
            while (this.starts.hasNext()) {
                Traverser.Admin<S> start = this.starts.next();
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
            }

            for (Traversal.Admin<S, ?> orTraversal : this.orTraversals) {
                for (Traverser.Admin<S> start : startRecordIds.values()) {
                    orTraversal.addStart(start);
                }
                while (orTraversal.hasNext()) {
                    Traverser.Admin<?> orTraverser = orTraversal.nextTraverser();
                    List<Object> filterTraverserObjects = orTraverser.path().objects();
                    String startId = "";
                    for (Object filteredTraverserObject : filterTraverserObjects) {
                        if (filteredTraverserObject instanceof Element) {
                            Element e = (Element) filteredTraverserObject;
                            startId += e.id().toString();
                        } else {
                            startId += filteredTraverserObject.toString();
                        }
                        if (startRecordIds.containsKey(startId)) {
                            this.results.addAll(startRecordIds.get(startId));
                            startRecordIds.removeAll(startId);
                        }
                        if (startRecordIds.isEmpty()) {
                            break;
                        }
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
    protected boolean filter(final Traverser.Admin<S> traverser) {
        for (final Traversal.Admin<S, ?> traversal : this.traversals) {
            if (TraversalUtil.test(traverser, traversal))
                return true;
        }
        return false;
    }

    @Override
    public void reset() {
        super.reset();
        this.first = true;
        this.results = new ArrayList<>();
        this.resultIterator = null;
        for (Traversal.Admin<S, ?> orTraversal : this.orTraversals) {
            orTraversal.reset();
        }
    }
}
