package org.umlg.sqlg.step.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.umlg.sqlg.step.SqlgFilterStep;
import org.umlg.sqlg.structure.traverser.SqlgTraverser;

import java.util.*;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2017/10/26
 */
public class SqlgNotStepBarrier<S> extends SqlgFilterStep<S> implements TraversalParent {

    private boolean first = true;
    private List<Traverser.Admin<S>> results = new ArrayList<>();
    private Iterator<Traverser.Admin<S>> resultIterator;
    private final Traversal.Admin<S, ?> notTraversal;

    public SqlgNotStepBarrier(final Traversal.Admin<?, ?> traversal, final Traversal<S, ?> notTraversal) {
        super(traversal);
        this.notTraversal = this.integrateChild(notTraversal.asAdmin());
    }

    @Override
    protected Traverser.Admin<S> processNextStart() {
        if (this.first) {
            this.first = false;
            Map<String, List<Traverser.Admin<S>>> startRecordIds = new HashMap<>();
            while (this.starts.hasNext()) {
                Traverser.Admin<S> start = this.starts.next();
                if (start instanceof SqlgTraverser) {
                    ((SqlgTraverser<?>) start).setRequiresOneBulk(true);
                }
                List<Object> startObjects = start.path().objects();
                StringBuilder recordIdConcatenated = new StringBuilder();
                for (Object startObject : startObjects) {
                    if (startObject instanceof Element e) {
                        recordIdConcatenated.append(e.id().toString());
                    } else {
                        recordIdConcatenated.append(startObject.toString());
                    }
                }
                startRecordIds.computeIfAbsent(recordIdConcatenated.toString(), (k) -> new ArrayList<>()).add(start);
                this.notTraversal.addStart(start);
            }

            while (this.notTraversal.hasNext()) {
                Traverser.Admin<?> traverser = this.notTraversal.nextTraverser();
                List<Object> filterTraverserObjects = traverser.path().objects();
                String startId = "";
                for (Object filteredTraverserObject : filterTraverserObjects) {
                    if (filteredTraverserObject instanceof Element e) {
                        startId += e.id().toString();
                    } else {
                        startId += filteredTraverserObject.toString();
                    }
                    startRecordIds.remove(startId);
                    if (startRecordIds.isEmpty()) {
                        break;
                    }
                }
            }

            Collection<List<Traverser.Admin<S>>> values = startRecordIds.values();
            for (List<Traverser.Admin<S>> value : values) {
                this.results.addAll(value);
            }
            this.results.sort((o1, o2) -> {
                SqlgTraverser<?> x = (SqlgTraverser<?>) o1;
                SqlgTraverser<?> y = (SqlgTraverser<?>) o2;
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
        return false;
    }

    @Override
    public void reset() {
        super.reset();
        this.first = true;
        this.results = new ArrayList<>();
        this.resultIterator = null;
        this.notTraversal.reset();
    }

    @Override
    public List<Traversal.Admin<S, ?>> getLocalChildren() {
        return Collections.singletonList(this.notTraversal);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, getLocalChildren());
    }
}
