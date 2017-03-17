package org.umlg.sqlg.process;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.strategy.SqlgComparatorHolder;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.ArrayList;
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
    private List<ReplacedStep<?, ?>> replacedSteps;
    private boolean hasStarted;
    private Emit<E> toEmit = null;
    private boolean eagerLoading = false;
    private List<Emit<E>> eagerLoadedResults;

    public SqlgRawIteratorToEmitIterator(List<ReplacedStep<?, ?>> replacedSteps, Supplier<Iterator<List<Emit<E>>>> supplier) {
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
            if (this.replacedSteps != null && this.replacedSteps.stream().anyMatch(r -> r.getSqlgComparatorHolder().hasComparators() || r.hasRange())) {
                this.eagerLoading = true;
                this.eagerLoadedResults = new ArrayList<>();
                eagerLoad();
                EmitOrderAndRangeHelper emitOrderAndRangeHelper = new EmitOrderAndRangeHelper<>(this.eagerLoadedResults, this.replacedSteps);
                emitOrderAndRangeHelper.sortAndApplyRange();
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
            return true;
        } else {
            return false;
        }
    }

    private void eagerLoad() {
        while (flattenRawIterator()) {
            this.eagerLoadedResults.add(this.toEmit);
            if (this.toEmit.isRepeat() && !this.toEmit.isRepeated()) {
                this.toEmit.setRepeated(true);
                this.eagerLoadedResults.add(this.toEmit);
            }
            this.toEmit = null;
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
            List<SqlgComparatorHolder> emitComparators = new ArrayList<>();
            Path currentPath = MutablePath.make();
            for (Emit<E> emit : emits) {
                this.toEmit = emit;
                if (!emit.isFake()) {
                    currentPath = currentPath.extend(emit.getElement(), emit.getLabels());
                    emitComparators.add(this.toEmit.getSqlgComparatorHolder());
                }
            }
            if (this.toEmit != null) {
                this.toEmit.setPath(currentPath);
                this.toEmit.setSqlgComparatorHolders(emitComparators);
            }
        }
        return this.toEmit != null;
    }

}
