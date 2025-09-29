package org.umlg.sqlg.util;

import org.apache.commons.lang3.tuple.MutablePair;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Pieter Martin
 * Date: 2021/01/26
 */
public class ThreadLocalMap<K, V> implements Map<K, V> {

    private final ThreadLocal<MutablePair<AtomicBoolean, Map<K, V>>> threadLocalMap = new ThreadLocal<>() {
        @Override
        protected MutablePair<AtomicBoolean, Map<K, V>> initialValue() {
            return MutablePair.of(new AtomicBoolean(true), null);
        }
    };

    @Override
    public int size() {
        MutablePair<AtomicBoolean, Map<K, V>> pair = this.threadLocalMap.get();
        if (pair.getLeft().get()) {
            return 0;
        } else {
            return pair.getRight().size();
        }
    }

    @Override
    public boolean isEmpty() {
        MutablePair<AtomicBoolean, Map<K, V>> pair = this.threadLocalMap.get();
        if (pair.getLeft().get()) {
            return true;
        } else {
            Preconditions.checkState(!pair.getRight().isEmpty());
            return false;
        }
    }

    @Override
    public boolean containsKey(Object key) {
        MutablePair<AtomicBoolean, Map<K, V>> pair = this.threadLocalMap.get();
        if (pair.getLeft().get()) {
            return false;
        } else {
            return pair.getRight().containsKey(key);
        }
    }

    @Override
    public boolean containsValue(Object value) {
        MutablePair<AtomicBoolean, Map<K, V>> pair = this.threadLocalMap.get();
        if (pair.getLeft().get()) {
            return false;
        } else {
            return pair.getRight().containsValue(value);
        }
    }

    @Override
    public V get(Object key) {
        MutablePair<AtomicBoolean, Map<K, V>> pair = this.threadLocalMap.get();
        if (pair.getLeft().get()) {
            return null;
        } else {
            return pair.getRight().get(key);
        }
    }

    @Override
    public V put(K key, V value) {
        MutablePair<AtomicBoolean, Map<K, V>> pair = this.threadLocalMap.get();
        if (pair.getLeft().get()) {
            pair.getLeft().set(false);
            pair.setRight(new HashMap<>());
        }
        return pair.getRight().put(key, value);
    }

    @Override
    public V remove(Object key) {
        MutablePair<AtomicBoolean, Map<K, V>> pair = this.threadLocalMap.get();
        if (pair.getLeft().get()) {
            return null;
        } else {
            V v = pair.getRight().remove(key);
            if (pair.getRight().isEmpty()) {
                pair.getLeft().set(true);
            }
            return v;
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        MutablePair<AtomicBoolean, Map<K, V>> pair = this.threadLocalMap.get();
        if (pair.getLeft().get()) {
            pair.getLeft().set(false);
            pair.setRight(new HashMap<>());
        }
        pair.getRight().putAll(m);
    }

    @Override
    public void clear() {
        MutablePair<AtomicBoolean, Map<K, V>> pair = this.threadLocalMap.get();
        if (!pair.getLeft().get()) {
            pair.getLeft().set(true);
            pair.getRight().clear();
        }
    }

    @Override
    public Set<K> keySet() {
        MutablePair<AtomicBoolean, Map<K, V>> pair = this.threadLocalMap.get();
        if (pair.getLeft().get()) {
            return Set.of();
        } else {
            return pair.getRight().keySet();
        }
    }

    @Override
    public Collection<V> values() {
        MutablePair<AtomicBoolean, Map<K, V>> pair = this.threadLocalMap.get();
        if (pair.getLeft().get()) {
            return List.of();
        } else {
            return pair.getRight().values();
        }
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        MutablePair<AtomicBoolean, Map<K, V>> pair = this.threadLocalMap.get();
        if (pair.getLeft().get()) {
            return Set.of();
        } else {
            return pair.getRight().entrySet();
        }
    }

    @Override
    public String toString() {
        MutablePair<AtomicBoolean, Map<K, V>> pair = this.threadLocalMap.get();
        if (pair.getLeft().get()) {
            return "{}";
        } else {
            return pair.getRight().toString();
        }
    }

}
