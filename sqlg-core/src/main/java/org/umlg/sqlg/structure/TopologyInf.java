package org.umlg.sqlg.structure;

/**
 * Created by pieter on 2016/12/08.
 */
public interface TopologyInf {

    default boolean isUncommitted() {
        return !isCommitted();
    }

    boolean isCommitted();

    String getName();

    /**
     * remove the topology item
     * @param preserveData if true we don't delete at the SQL level
     */
    void remove(boolean preserveData);

    default void remove() {
        remove(false);
    }

    default void rename(String name) {
        throw SqlgExceptions.topologyRenameNotSupported(this.getClass().getSimpleName());
    }
}
