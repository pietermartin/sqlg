package org.umlg.sqlg.util;

import java.util.*;

/**
 * @author Pieter Martin
 * Date: 2021/01/26
 */
public class ThreadLocalList<E> implements List<E> {

    private final ThreadLocal<List<E>> threadLocalList = ThreadLocal.withInitial(ArrayList::new);

    @Override
    public int size() {
        return this.threadLocalList.get().size();
    }

    @Override
    public boolean isEmpty() {
        return this.threadLocalList.get().isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return this.threadLocalList.get().contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return this.threadLocalList.get().iterator();
    }

    @Override
    public Object[] toArray() {
        return this.threadLocalList.get().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return this.threadLocalList.get().toArray(a);
    }

    @Override
    public boolean add(E e) {
        return this.threadLocalList.get().add(e);
    }

    @Override
    public boolean remove(Object o) {
        return this.threadLocalList.get().remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return this.threadLocalList.get().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return this.threadLocalList.get().addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        return this.threadLocalList.get().addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return this.threadLocalList.get().removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return this.threadLocalList.get().retainAll(c);
    }

    @Override
    public void clear() {
        this.threadLocalList.get().clear();
    }

    @Override
    public E get(int index) {
        return this.threadLocalList.get().get(index);
    }

    @Override
    public E set(int index, E element) {
        return this.threadLocalList.get().set(index, element);
    }

    @Override
    public void add(int index, E element) {
        this.threadLocalList.get().add(index, element);
    }

    @Override
    public E remove(int index) {
        return this.threadLocalList.get().remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return this.threadLocalList.get().indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return this.threadLocalList.get().lastIndexOf(o);
    }

    @Override
    public ListIterator<E> listIterator() {
        return this.threadLocalList.get().listIterator();
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        return this.threadLocalList.get().listIterator(index);
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        return this.threadLocalList.get().subList(fromIndex, toIndex);
    }
}
