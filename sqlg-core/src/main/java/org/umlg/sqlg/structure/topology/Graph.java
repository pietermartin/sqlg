package org.umlg.sqlg.structure.topology;

import org.umlg.sqlg.structure.TopologyInf;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2017/11/14
 */
public class Graph implements TopologyInf {

    @Override
    public String getName() {
        return "Graph";
    }

    @Override
    public void remove(boolean preserveData) {
        throw new IllegalStateException("Graph can not be removed!");
    }
}
