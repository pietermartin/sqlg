package org.umlg.sqlg.structure;

/**
 * Created by pieter on 2016/12/08.
 */
public interface TopologyInf {

    boolean isUncommitted();

    default boolean isCommitted() {
        return !isUncommitted();
    }

}
