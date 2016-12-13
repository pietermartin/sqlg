package org.umlg.sqlg.structure;

/**
 * Created by pieter on 2016/12/08.
 */
public interface TopologyInf {

    default boolean isUncommitted() {
        return !isCommitted();
    }

    boolean isCommitted();

}
