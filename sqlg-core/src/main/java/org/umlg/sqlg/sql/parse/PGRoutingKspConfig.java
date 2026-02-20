package org.umlg.sqlg.sql.parse;

import org.umlg.sqlg.services.SqlgPGRoutingFactory;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.util.Preconditions;

import java.util.List;

public record PGRoutingKspConfig(String name, List<Long> start_vids, List<Long> end_vids, Integer k, boolean directed,
                                 VertexLabel vertexLabel,
                                 EdgeLabel edgeLabel) {

    public PGRoutingKspConfig(String name, List<Long> start_vids, List<Long> end_vids, Integer k, boolean directed, VertexLabel vertexLabel, EdgeLabel edgeLabel) {
        this.name = name;
        this.start_vids = start_vids;
        this.end_vids = end_vids;
        this.k = k;
        this.directed = directed;
        this.vertexLabel = vertexLabel;
        this.edgeLabel = edgeLabel;
        Preconditions.checkState(name.equals(SqlgPGRoutingFactory.pgr_dijkstra));
        Preconditions.checkState(!start_vids.isEmpty());
        Preconditions.checkState(!end_vids.isEmpty());
    }

    public String toStartVidsString() {
        if (start_vids().size() == 1) {
            return start_vids().get(0).toString();
        } else {
            return "ARRAY[" + String.join(",", start_vids().stream().map(Object::toString).toArray(String[]::new)) + "]";
        }
    }

    public String toEndVidsString() {
        if (end_vids().size() == 1) {
            return end_vids().get(0).toString();
        } else {
            return "ARRAY[" + String.join(",", end_vids().stream().map(Object::toString).toArray(String[]::new)) + "]";
        }
    }

}
