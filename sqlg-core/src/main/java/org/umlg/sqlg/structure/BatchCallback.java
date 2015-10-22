package org.umlg.sqlg.structure;

import java.util.List;

public interface BatchCallback<T extends SqlgElement> {
    /**
     * A callback for when the batch size is reached and the output stream is flushed.
     * @param sqlgElement A reference to the all the elements in the batch.
     */
    void callBack(List<T> sqlgElement);
}
