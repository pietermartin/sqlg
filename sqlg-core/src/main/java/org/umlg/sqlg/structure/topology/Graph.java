package org.umlg.sqlg.structure.topology;

import org.umlg.sqlg.structure.TopologyInf;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/11/14
 */
public class Graph implements TopologyInf {

    @Override
    public boolean isCommitted() {
        return true;
    }

    @Override
    public String getName() {
        return "Graph";
    }

    @Override
    public void remove(boolean preserveData) {
        throw new IllegalStateException("Graph can not be removed!");
    }
}
