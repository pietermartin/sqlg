package org.umlg.sqlg.ui.util;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections4.set.ListOrderedSet;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Pieter Martin
 * Date: 2019/10/05
 */
public class SlickLazyTree {

    private Object cache;
    private ObjectNode entry;
    private SlickLazyTree parent;
    private final ListOrderedSet<SlickLazyTree> children = new ListOrderedSet<>();
    private final Map<String, SlickLazyTree> childrenMap = new HashMap<>();
    private boolean childrenIsLoaded = false;

    public SlickLazyTree() {
    }

    private SlickLazyTree(ObjectNode entry) {
        this.entry = entry;
    }

    public static SlickLazyTree from(ObjectNode entry) {
        return new SlickLazyTree(entry);
    }

    public ObjectNode getEntry() {
        return entry;
    }

    public void setEntry(ObjectNode entry) {
        this.entry = entry;
    }

    public Object getCache() {
        return cache;
    }

    public void setCache(Object cache) {
        this.cache = cache;
    }

    public String getId() {
        return this.entry.get("id").asText();
    }

    public void setParent(SlickLazyTree parent) {
        this.parent = parent;
    }

    public SlickLazyTree getParent() {
        return parent;
    }

    public String getEntryParentField() {
        if (this.entry.hasNonNull("parent")) {
            return this.entry.get("parent").asText();
        } else {
            return null;
        }
    }

    public ListOrderedSet<SlickLazyTree> getChildren() {
        return children;
    }

    public Map<String, SlickLazyTree> getChildrenMap() {
        return childrenMap;
    }

    public boolean isChildrenIsLoaded() {
        return childrenIsLoaded;
    }

    public void setChildrenIsLoaded(boolean childrenIsLoaded) {
        this.childrenIsLoaded = childrenIsLoaded;
    }

    public void addChild(SlickLazyTree child) {
        this.children.add(child);
        this.childrenMap.put(child.getId(), child);
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SlickLazyTree)) {
            return false;
        }
        SlickLazyTree o = (SlickLazyTree)other;
        return getId().equals(o.getId());
    }

    @Override
    public String toString() {
        return getId();
    }
}
