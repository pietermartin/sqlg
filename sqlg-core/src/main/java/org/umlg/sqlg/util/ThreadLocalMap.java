package org.umlg.sqlg.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Pieter Martin
 * Date: 2021/01/26
 */
public class ThreadLocalMap<K, V> implements Map<K, V> {

    private final ThreadLocal<Map<K, V>> threadLocalMap = ThreadLocal.withInitial(HashMap::new);

    @Override
    public int size() {
        return this.threadLocalMap.get().size();
    }

    @Override
    public boolean isEmpty() {
        return this.threadLocalMap.get().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return this.threadLocalMap.get().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return this.threadLocalMap.get().containsValue(value);
    }

    @Override
    public V get(Object key) {
        return this.threadLocalMap.get().get(key);
    }

    @Override
    public V put(K key, V value) {
        return this.threadLocalMap.get().put(key, value);
    }

    @Override
    public V remove(Object key) {
        return this.threadLocalMap.get().remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        this.threadLocalMap.get().putAll(m);
    }

    @Override
    public void clear() {
        this.threadLocalMap.get().clear();
    }

    @Override
    public Set<K> keySet() {
        return this.threadLocalMap.get().keySet();
    }

    @Override
    public Collection<V> values() {
        return this.threadLocalMap.get().values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return this.threadLocalMap.get().entrySet();
    }

}
