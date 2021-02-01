package org.umlg.sqlg.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Pieter Martin
 * Date: 2021/01/26
 */
public class ThreadLocalSet<E> implements Set<E> {

    private final ThreadLocal<Set<E>> threadLocalSet = ThreadLocal.withInitial(HashSet::new);

    @Override
    public int size() {
        return this.threadLocalSet.get().size();
    }

    @Override
    public boolean isEmpty() {
        return this.threadLocalSet.get().isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return this.threadLocalSet.get().contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return this.threadLocalSet.get().iterator();
    }

    @Override
    public Object[] toArray() {
        return this.threadLocalSet.get().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return this.threadLocalSet.get().toArray(a);
    }

    @Override
    public boolean add(E e) {
        return this.threadLocalSet.get().add(e);
    }

    @Override
    public boolean remove(Object o) {
        return this.threadLocalSet.get().remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return this.threadLocalSet.get().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return this.threadLocalSet.get().addAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return this.threadLocalSet.get().retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return this.threadLocalSet.get().removeAll(c);
    }

    @Override
    public void clear() {
        this.threadLocalSet.get().clear();
    }
}
