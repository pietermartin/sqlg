package org.umlg.sqlg.sql.parse;

import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;

import java.util.List;

public record PGRoutingConfig(String name, List<Long> start_vids, List<Long> end_vids, boolean directed,
                              VertexLabel vertexLabel,
                              EdgeLabel edgeLabel) {

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
