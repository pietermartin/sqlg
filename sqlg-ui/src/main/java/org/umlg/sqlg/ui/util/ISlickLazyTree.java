package org.umlg.sqlg.ui.util;

import org.apache.commons.collections4.set.ListOrderedSet;

public interface ISlickLazyTree {

    SlickLazyTree parent(SlickLazyTree entry);

    ListOrderedSet<SlickLazyTree> children(SlickLazyTree parent);

    void refresh(SlickLazyTree slickLazyTree);

}
