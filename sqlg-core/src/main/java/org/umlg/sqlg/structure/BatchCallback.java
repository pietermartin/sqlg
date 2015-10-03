package org.umlg.sqlg.structure;

public interface BatchCallback<T extends SqlgElement> {
    /**
     * A callback for when the batch size is reached and the output stream is flushed.
     * @param sqlgElement A reference to the last element in the batch.
     *                    This is needed as the client will not have a reference to it as the callBack is called
     *                    before the method returns with the element.
     */
    void callBack(T sqlgElement);
}
