package org.umlg.sqlg.step.barrier;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.collections4.CollectionUtils;
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
public class SqlgAndStepBarrier<S> extends SqlgConnectiveStep<S> {

    private boolean first = true;
    private List<Traverser.Admin<S>> results = new ArrayList<>();
    private Iterator<Traverser.Admin<S>> resultIterator;
    private final List<Traversal.Admin<S, ?>> andTraversals = new ArrayList<>();

    public SqlgAndStepBarrier(final Traversal.Admin traversal, final Collection<Traversal<S, ?>> traversals) {
        super(traversal, traversals);
        for (Traversal<S, ?> sTraversal : traversals) {
            //noinspection unchecked
            this.andTraversals.add((Traversal.Admin<S, ?>) sTraversal);
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

            boolean first = true;
            for (Traversal.Admin<S, ?> andTraversal : this.andTraversals) {
                for (Traverser.Admin<S> start : startRecordIds.values()) {
                    andTraversal.addStart(start);
                }
                //Need to use a tmp map here to prevent duplicates.
                //If the and traversals yields multiple results the start must only be added once.
                Multimap<String, Traverser.Admin<S>> tmpStartRecordIds = LinkedListMultimap.create();
                tmpStartRecordIds.putAll(startRecordIds);
                List<Traverser.Admin<S>> tmpResult = new ArrayList<>();
                while (andTraversal.hasNext()) {
                    Traverser.Admin<?> filterTraverser = andTraversal.nextTraverser();
                    List<Object> filterTraverserObjects = filterTraverser.path().objects();
                    String startId = "";
                    for (Object filteredTraverserObject : filterTraverserObjects) {
                        if (filteredTraverserObject instanceof Element) {
                            Element e = (Element) filteredTraverserObject;
                            startId += e.id().toString();
                        } else {
                            startId += filteredTraverserObject.toString();
                        }
                        if (tmpStartRecordIds.containsKey(startId)) {
                            if (first) {
                                this.results.addAll(tmpStartRecordIds.get(startId));
                            } else {
                                tmpResult.addAll(tmpStartRecordIds.get(startId));
                            }
                            tmpStartRecordIds.removeAll(startId);
                        }
                    }
                }
                if (!first) {
                    this.results = new ArrayList<>(CollectionUtils.intersection(this.results, tmpResult));
                }
                first = false;
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
        for (Traversal.Admin<S, ?> andTraversal : this.andTraversals) {
            andTraversal.reset();
        }
    }
}
