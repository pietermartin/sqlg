package org.umlg.sqlg.util;

import org.apache.commons.collections4.set.ListOrderedSet;

import java.util.*;

/**
 * @author Pieter Martin
 * Date: 2021/01/26
 */
public class ThreadLocalListOrderedSet<E> implements Set<E> {

    private final ThreadLocal<Set<E>> threadLocalListOrderedSet = ThreadLocal.withInitial(ListOrderedSet::new);

    @Override
    public int size() {
        return this.threadLocalListOrderedSet.get().size();
    }

    @Override
    public boolean isEmpty() {
        return this.threadLocalListOrderedSet.get().isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return this.threadLocalListOrderedSet.get().contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return this.threadLocalListOrderedSet.get().iterator();
    }

    @Override
    public Object[] toArray() {
        return this.threadLocalListOrderedSet.get().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return this.threadLocalListOrderedSet.get().toArray(a);
    }

    @Override
    public boolean add(E e) {
        return this.threadLocalListOrderedSet.get().add(e);
    }

    @Override
    public boolean remove(Object o) {
        return this.threadLocalListOrderedSet.get().remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return this.threadLocalListOrderedSet.get().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return this.threadLocalListOrderedSet.get().addAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return this.threadLocalListOrderedSet.get().retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return this.threadLocalListOrderedSet.get().removeAll(c);
    }

    @Override
    public void clear() {
        this.threadLocalListOrderedSet.get().clear();
    }
}
