package org.umlg.sqlg.step.barrier;

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
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2017/08/31
 */
public class SqlgOptionalStepBarrier<S> extends SqlgAbstractStep<S, S> implements TraversalParent {

    private final Traversal.Admin<S, S> optionalTraversal;
    private final List<Traverser.Admin<S>> results = new ArrayList<>();
    private boolean first = true;
    private Iterator<Traverser.Admin<S>> resultIterator;

    public SqlgOptionalStepBarrier(final Traversal.Admin traversal, final Traversal.Admin<S, S> optionalTraversal) {
        super(traversal);
        this.optionalTraversal = optionalTraversal;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        if (this.first) {
            Map<String, List<Traverser.Admin<S>>> startRecordIds = new HashMap<>();
            this.first = false;
            long startCount = 1;

            while (this.starts.hasNext()) {
                Traverser.Admin<S> start = this.starts.next();
                this.optionalTraversal.addStart(start);
                List<Object> startObjects = start.path().objects();
                StringBuilder recordIdConcatenated = new StringBuilder();
                for (Object startObject : startObjects) {
                    Element e = (Element) startObject;
                    recordIdConcatenated.append(e.id().toString());
                }
                startRecordIds.computeIfAbsent(recordIdConcatenated.toString(), (k) -> new ArrayList<>()).add(start);
            }
            while (true) {
                if (this.optionalTraversal.hasNext()) {
                    Traverser.Admin<S> optionalTraverser = this.optionalTraversal.nextTraverser();
                    this.results.add(optionalTraverser);
                    List<Object> optionObjects = optionalTraverser.path().objects();
                    String startId = "";
                    for (Object optionObject : optionObjects) {
                        Element e = (Element) optionObject;
                        startId += e.id().toString();
                        List<Traverser.Admin<S>> removed = startRecordIds.remove(startId);
                        if (removed != null && !removed.isEmpty()) {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }

            for (String key : startRecordIds.keySet()) {
                List<Traverser.Admin<S>> values = startRecordIds.get(key);
                for (Traverser.Admin<S> start : values) {
                    this.results.add(start);
                    //Bulking logic interferes here, addStart calls DefaultTraversal.merge which has bulking logic
                    start.setBulk(1L);
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
        if (this.resultIterator.hasNext()) {
            return this.resultIterator.next();
        }
        throw FastNoSuchElementException.instance();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Traversal.Admin<S, S>> getLocalChildren() {
        return Collections.singletonList(this.optionalTraversal);
    }

    @SuppressWarnings("Duplicates")
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
    public String toString() {
        return StringFactory.stepString(this, this.optionalTraversal);
    }
}
