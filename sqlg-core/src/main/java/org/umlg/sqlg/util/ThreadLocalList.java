package org.umlg.sqlg.util;

import javax.annotation.Nonnull;
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
    @Nonnull
    public Iterator<E> iterator() {
        return this.threadLocalList.get().iterator();
    }

    @Override
    @Nonnull
    public Object[] toArray() {
        return this.threadLocalList.get().toArray();
    }

    @Override
    @Nonnull
    public <T> T[] toArray(@Nonnull T[] a) {
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
    public boolean containsAll(@Nonnull Collection<?> c) {
        return this.threadLocalList.get().containsAll(c);
    }

    @Override
    public boolean addAll(@Nonnull Collection<? extends E> c) {
        return this.threadLocalList.get().addAll(c);
    }

    @Override
    public boolean addAll(int index, @Nonnull Collection<? extends E> c) {
        return this.threadLocalList.get().addAll(c);
    }

    @Override
    public boolean removeAll(@Nonnull Collection<?> c) {
        return this.threadLocalList.get().removeAll(c);
    }

    @Override
    public boolean retainAll(@Nonnull Collection<?> c) {
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
    @Nonnull
    public ListIterator<E> listIterator() {
        return this.threadLocalList.get().listIterator();
    }

    @Override
    @Nonnull
    public ListIterator<E> listIterator(int index) {
        return this.threadLocalList.get().listIterator(index);
    }

    @Override
    @Nonnull
    public List<E> subList(int fromIndex, int toIndex) {
        return this.threadLocalList.get().subList(fromIndex, toIndex);
    }
}
