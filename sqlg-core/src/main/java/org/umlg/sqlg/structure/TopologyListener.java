package org.umlg.sqlg.structure;

/**
 * Date: 2017/01/22
 * Time: 6:39 PM
 */
public interface TopologyListener {

    void change(TopologyInf topologyInf, TopologyInf oldValue, TopologyChangeAction action);
}
