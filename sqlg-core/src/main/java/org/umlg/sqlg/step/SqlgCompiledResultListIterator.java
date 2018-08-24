package org.umlg.sqlg.step;

import org.umlg.sqlg.structure.SqlgCompiledResultIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/04/27
 */
class SqlgCompiledResultListIterator<E> implements ListIterator<E> {

    
    private final List<E> internalList = new ArrayList<>();
    private final ListIterator<E> internalListIterator;

    public SqlgCompiledResultListIterator(SqlgCompiledResultIterator<E> sqlgCompiledResultIterator) {
        sqlgCompiledResultIterator.forEachRemaining(e -> this.internalList.add(e));
        this.internalListIterator = this.internalList.listIterator();
    }

    @Override
    public boolean hasNext() {
        return this.internalListIterator.hasNext();
    }

    @Override
    public E next() {
        return this.internalListIterator.next();
    }

    @Override
    public boolean hasPrevious() {
        return this.internalListIterator.hasPrevious();
    }

    @Override
    public E previous() {
        return this.internalListIterator.previous();
    }

    @Override
    public int nextIndex() {
        return this.internalListIterator.nextIndex();
    }

    @Override
    public int previousIndex() {
        return this.internalListIterator.previousIndex();
    }

    @Override
    public void remove() {
        this.internalListIterator.remove();
    }

    @Override
    public void set(E e) {
        this.internalListIterator.set(e);
    }

    @Override
    public void add(E e) {

    }
}
