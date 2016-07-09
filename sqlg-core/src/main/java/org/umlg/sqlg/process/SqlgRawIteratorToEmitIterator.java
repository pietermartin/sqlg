package org.umlg.sqlg.process;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
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
public class SqlgRawIteratorToEmitIterator<E extends SqlgElement> implements Iterator<Emit<E>>, Supplier<Iterator<Emit<E>>> {

    private Supplier<Iterator<List<Emit<E>>>> supplier;
    private Iterator<List<Emit<E>>> iterator;
    private boolean hasStarted;
    private List<Emit<E>> toEmit = new ArrayList<>();

    public SqlgRawIteratorToEmitIterator(Supplier<Iterator<List<Emit<E>>>> supplier) {
        this.supplier = supplier;
    }

    public SqlgRawIteratorToEmitIterator(Iterator<List<Emit<E>>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        if (!this.hasStarted) {
            if (this.iterator == null) {
                this.iterator = this.supplier.get();
            }
            this.hasStarted = true;
            return toEmitTree();
        } else {
            return !this.toEmit.isEmpty() || toEmitTree();
        }
    }

    @Override
    public Emit<E> next() {
        return this.toEmit.remove(0);
    }

    private boolean toEmitTree() {

        boolean result = false;
        while (!result) {
            List<Emit<E>> flattenedEmit = flattenRawIterator();
            if (!flattenedEmit.isEmpty()) {
                Iterator<Emit<E>> iter = flattenedEmit.iterator();
                while (iter.hasNext()) {
                    Emit<E> emit = iter.next();
                    iter.remove();
                    this.toEmit.add(emit);
                    result = true;
                }
            } else {
                //no more raw results so exit the loop. result will be false;
                break;
            }
        }
        return result;
    }

    private List<Emit<E>> flattenRawIterator() {
        if (this.iterator.hasNext()) {
            List<Emit<E>> flattenedEmit = new ArrayList<>();
            List<Emit<E>> emits = this.iterator.next();
            Emit<E> toEmit = null;
            Path currentPath = MutablePath.make();
            for (Emit<E> emit : emits) {
                toEmit = emit;
                if (!emit.isFake()) {
                    currentPath = currentPath.extend(emit.getElement(), emit.getLabels());
                }
            }
            if (toEmit != null) {
                toEmit.setPath(currentPath);
                flattenedEmit.add(toEmit);
                if (toEmit.isRepeat()) {
                    flattenedEmit.add(toEmit);
                }
            }
            return flattenedEmit;

        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public Iterator<Emit<E>> get() {
        return this;
    }

}
