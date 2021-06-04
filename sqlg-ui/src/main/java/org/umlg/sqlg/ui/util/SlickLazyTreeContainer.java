package org.umlg.sqlg.ui.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Pieter Martin
 * Date: 2019/10/05
 */
public class SlickLazyTreeContainer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SlickLazyTreeContainer.class.getSimpleName());
    private final ListOrderedSet<SlickLazyTree> roots;
    private final Map<String, SlickLazyTree> entries = new HashMap<>();
    private final ListOrderedSet<SlickLazyTree> selectedEntries = new ListOrderedSet<>();
    private ISlickLazyTree iSlickLazyTree;

    public SlickLazyTreeContainer(ListOrderedSet<SlickLazyTree> roots) {
        this.roots = roots;
    }

    public void addSelectedEntry(SlickLazyTree selectedEntry) {
        this.selectedEntries.add(selectedEntry);
    }

    public ArrayNode complete(ISlickLazyTree iSlickLazyTree) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        LOGGER.debug("SlickLazyTreeContainer start");
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        this.iSlickLazyTree = iSlickLazyTree;
        for (SlickLazyTree selectedEntry : this.selectedEntries) {
            selectedEntry.getEntry().put("selected", true);
            selectedEntry.getEntry().put("_fetched", false);
            selectedEntry.getEntry().set("children", objectMapper.createArrayNode());
            if (!this.entries.containsKey(selectedEntry.getId())) {
                selectedEntry.getEntry().put("_collapsed", true);
                this.iSlickLazyTree.refresh(selectedEntry);
            } else {
                selectedEntry.getEntry().put("_collapsed", false);
            }
            walkParent(selectedEntry);
        }
        ArrayNode flattenedRoots = flattenRoots();
        stopWatch.stop();
        LOGGER.debug(String.format("SlickLazyTreeContainer finished. Time taken: %s", stopWatch.toString()));
        return flattenedRoots;
    }

    private ArrayNode flattenRoots() {
        ObjectMapper objectMapper = ObjectMapperFactory.INSTANCE.getObjectMapper();
        ArrayNode result = objectMapper.createArrayNode();
        for (SlickLazyTree root : this.roots) {
            walkChildren(root, result);
            root.getEntry().putNull("parent");
        }
        return result;
    }

    private void walkChildren(SlickLazyTree parent, ArrayNode result) {
        result.add(parent.getEntry());
        if (!parent.getChildren().isEmpty()) {
            parent.getEntry().put("_fetched", true);
        }
        for (SlickLazyTree child : parent.getChildren()) {
            child.getEntry().put("parent", parent.getId());
            ArrayNode children = (ArrayNode) parent.getEntry().get("children");
            children.add(child.getId());
            walkChildren(child, result);
        }
    }

    private void walkParent(SlickLazyTree entry) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        LOGGER.debug(String.format("walkParent for %s", entry.getId()));
        SlickLazyTree parent = this.entries.get(entry.getEntryParentField());
        if (parent == null) {
            LOGGER.debug("start iSlickLazyTree.parent");
            parent = this.iSlickLazyTree.parent(entry);
            stopWatch.stop();
            LOGGER.debug(String.format("finish iSlickLazyTree.parent for %s", stopWatch.toString()));
            stopWatch.reset();
            stopWatch.start();
        }
        if (parent != null) {
            parent.getEntry().put("_collapsed", false);
            if (!this.entries.containsKey(parent.getId())) {
                this.entries.put(parent.getId(), parent);
                if (!parent.isChildrenIsLoaded()) {
                    LOGGER.debug(String.format("start iSlickLazyTree.children 1 for parent %s", parent.getId()));
                    ListOrderedSet<SlickLazyTree> children = this.iSlickLazyTree.children(parent);
                    stopWatch.stop();
                    LOGGER.debug(String.format("finish iSlickLazyTree.children 1 for parent %s, Time taken: %s", parent.getId(), stopWatch.toString()));
                    stopWatch.reset();
                    stopWatch.start();
                    for (SlickLazyTree child : children) {
                        if (child.getId().equals(entry.getId())) {
                            child = entry;
                        }
                        parent.addChild(child);
                        child.setParent(parent);
                        if (!this.entries.containsKey(child.getId())) {
                            this.entries.put(child.getId(), child);
                        }
                    }
                    parent.setChildrenIsLoaded(true);
                } else {
                    parent.getChildrenMap().get(entry.getId()).setEntry(entry.getEntry());
                }
            } else {
                //If the parent is already present, check if its children are loaded. If not load them.
                if (!parent.isChildrenIsLoaded()) {
                    stopWatch.stop();
                    LOGGER.debug(String.format("start iSlickLazyTree.children 2 for parent %s", parent.getId()));
                    stopWatch.reset();
                    stopWatch.start();
                    ListOrderedSet<SlickLazyTree> children = this.iSlickLazyTree.children(parent);
                    LOGGER.debug(String.format("finish iSlickLazyTree.children 2 for parent %s, Time taken: %s", parent.getId(), stopWatch.toString()));
                    stopWatch.reset();
                    stopWatch.start();
                    for (SlickLazyTree child : children) {
                        if (child.getId().equals(entry.getId())) {
                            child = entry;
                        }
                        parent.addChild(child);
                        child.setParent(parent);
                        if (!this.entries.containsKey(child.getId())) {
                            this.entries.put(child.getId(), child);
                        }
                    }
                    parent.setChildrenIsLoaded(true);
                } else {
                    parent.getChildrenMap().get(entry.getId()).setEntry(entry.getEntry());
                }
            }
            walkParent(parent);
        } else {
            this.entries.put(entry.getId(), entry);
        }
    }

}
