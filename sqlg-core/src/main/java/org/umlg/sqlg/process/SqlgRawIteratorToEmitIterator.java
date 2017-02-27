package org.umlg.sqlg.process;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * Date: 2016/04/04
 * Time: 8:54 PM
 */
public class SqlgRawIteratorToEmitIterator<S extends SqlgElement, E extends SqlgElement> implements Iterator<Emit<E>>, Supplier<Iterator<Emit<E>>> {

    private Supplier<Iterator<List<Emit<E>>>> supplier;
    private Iterator<List<Emit<E>>> iterator;
    private List<ReplacedStep<S, E>> replacedSteps;
    private boolean hasStarted;
    private Emit<E> toEmit = null;
    private boolean eagerLoading = false;
    private List<Emit<E>> eagerLoadedResults;

    public SqlgRawIteratorToEmitIterator(List<ReplacedStep<S, E>> replacedSteps, Supplier<Iterator<List<Emit<E>>>> supplier) {
        this.supplier = supplier;
        this.replacedSteps = replacedSteps;
    }

    public SqlgRawIteratorToEmitIterator(Iterator<List<Emit<E>>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public Iterator<Emit<E>> get() {
        return this;
    }

    @Override
    public boolean hasNext() {
        if (!this.hasStarted) {
            if (this.iterator == null) {
                this.iterator = this.supplier.get();
            }
            this.hasStarted = true;
            //For now order() means eager load the whole query and then sort it.
            if (this.replacedSteps != null && this.replacedSteps.stream().anyMatch(r -> !r.getComparators().isEmpty())) {
                this.eagerLoading = true;
                this.eagerLoadedResults = new ArrayList<>();
                eagerLoad();
                Collections.sort(this.eagerLoadedResults, new EmitComparator<>(this.replacedSteps));
                return eagerLoadHasNext();
            } else {
                return flattenRawIterator();
            }
        } else {
            if (!this.eagerLoading) {
                return this.toEmit != null || flattenRawIterator();
            } else {
                return eagerLoadHasNext();
            }
        }
    }

    private boolean eagerLoadHasNext() {
        if (!this.eagerLoadedResults.isEmpty()) {
            this.toEmit = this.eagerLoadedResults.remove(0);
            ReplacedStep replacedStep = this.replacedSteps.get(toEmit.getPath().size() - 1);
            if (replacedStep.hasRange()) {
                if (replacedStep.containsRange()) {
                    return true;
                } else {
                    //iterate till there is something to emit
                    while (true) {
                        if (!this.eagerLoadedResults.isEmpty()) {
                            this.toEmit = this.eagerLoadedResults.remove(0);
                            replacedStep = this.replacedSteps.get(toEmit.getPath().size() - 1);
                            if (replacedStep.hasRange()) {
                                if (replacedStep.containsRange()) {
                                    return true;
                                }
                            } else {
                                return true;
                            }
                        } else {
                            return false;
                        }
                    }
                }
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    private void eagerLoad() {
        while (flattenRawIterator()) {
            if (this.toEmit != null) {
                this.eagerLoadedResults.add(this.toEmit);
                if (this.toEmit.isRepeat() && !this.toEmit.isRepeated()) {
                    this.toEmit.setRepeated(true);
                    this.eagerLoadedResults.add(this.toEmit);
                }
                this.toEmit = null;
            }
        }
    }

    @Override
    public Emit<E> next() {
        if (!this.eagerLoading) {
            Emit<E> result = this.toEmit;
            if (this.toEmit.isRepeat() && !this.toEmit.isRepeated()) {
                this.toEmit.setRepeated(true);
            } else {
                this.toEmit = null;
            }
            return result;
        } else {
            return this.toEmit;
        }
    }

    private boolean flattenRawIterator() {
        if (this.iterator.hasNext()) {
            List<Emit<E>> emits = this.iterator.next();
            Path currentPath = MutablePath.make();
            for (Emit<E> emit : emits) {
                this.toEmit = emit;
                if (!emit.isFake()) {
                    currentPath = currentPath.extend(emit.getElement(), emit.getLabels());
                }
            }
            if (this.toEmit != null) {
                this.toEmit.setPath(currentPath);
            }
        }
        return this.toEmit != null;
    }

}
