package org.umlg.sqlg.step;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.structure.SqlgTraverser;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/08/31
 */
public class SqlgOptionalStepBarrier<S> extends SqlgAbstractStep<S, S> implements TraversalParent {

    private final Traversal.Admin<S, S> optionalTraversal;
    protected List<Traverser.Admin<S>> results = new ArrayList<>();
    private boolean first = true;
    private Iterator<Traverser.Admin<S>> resultIterator;

    public SqlgOptionalStepBarrier(final Traversal.Admin traversal, final Traversal.Admin<S, S> optionalTraversal) {
        super(traversal);
        this.optionalTraversal = optionalTraversal;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        if (this.first) {
            Multimap<String, Traverser.Admin<S>> startRecordIds = LinkedListMultimap.create();
            this.first = false;
            long startCount = 1;

            while (this.starts.hasNext()) {
                Traverser.Admin<S> start = this.starts.next();
                this.optionalTraversal.addStart(start);
                ((SqlgElement) start.get()).setInternalStartTraverserIndex(startCount++);
                List<Object> startObjects = start.path().objects();
                StringBuilder recordIdConcatenated = new StringBuilder();
                for (Object startObject : startObjects) {
                    Element e = (Element) startObject;
                    recordIdConcatenated.append(e.id().toString());
                }
                startRecordIds.put(recordIdConcatenated.toString(), start);
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
                        if (startRecordIds.removeAll(startId).isEmpty()) {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }

            for (Traverser.Admin<S> start : startRecordIds.values()) {
                this.results.add(start);
                //Bulking logic interferes here, addStart calls DefaultTraversal.merge which has bulking logic
                start.setBulk(1L);
            }
            //Now travers the options. The starts have been set.
            while (true) {
                if (this.optionalTraversal.hasNext()) {
                    this.results.add(optionalTraversal.nextTraverser());
                } else {
                    break;
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
    public List<Traversal.Admin<S, S>> getLocalChildren() {
        return Collections.singletonList(this.optionalTraversal);
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

}
