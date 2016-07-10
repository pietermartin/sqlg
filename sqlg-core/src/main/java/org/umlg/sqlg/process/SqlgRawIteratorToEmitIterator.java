package org.umlg.sqlg.process;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * Date: 2016/04/04
 * Time: 8:54 PM
 */
public class SqlgRawIteratorToEmitIterator<E extends SqlgElement> implements Iterator<Emit<E>>, Supplier<Iterator<Emit<E>>> {

    private Supplier<Iterator<List<Emit<E>>>> supplier;
    private Iterator<List<Emit<E>>> iterator;
    private boolean hasStarted;
    private Emit<E> toEmit = null;

    public SqlgRawIteratorToEmitIterator(Supplier<Iterator<List<Emit<E>>>> supplier) {
        this.supplier = supplier;
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
            return flattenRawIterator();
        } else {
            return this.toEmit != null || flattenRawIterator();
        }
    }

    @Override
    public Emit<E> next() {
        Emit<E> result = this.toEmit;
        if (this.toEmit.isRepeat() && !this.toEmit.isRepeated()) {
            this.toEmit.setRepeated(true);
        } else {
            this.toEmit = null;
        }
        return result;
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
