package org.umlg.sqlg.structure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Date: 2015/07/01
 * Time: 2:03 PM
 */
public class SqlgCompiledResultIterator<E> implements Iterator<E> {

    private List<E> internalElement = new ArrayList<>();
    private Iterator<E> iterator;

    @Override
    public boolean hasNext() {
        if (this.iterator == null) {
            this.iterator = this.internalElement.iterator();
        }
        return this.iterator.hasNext();
    }

    @Override
    public E next() {
        if (this.iterator == null) {
            this.iterator = this.internalElement.iterator();
        }
        return this.iterator.next();
    }

    public void add(E e) {
        this.internalElement.add(e);
    }

    public void addLabeledElement(E e) {
        this.internalElement.add(e);
    }
}
