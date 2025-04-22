package org.umlg.sqlg.sql.parse;

import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;

public record PGRoutingConfig(String name, long start_vid, long end_vid, boolean directed, VertexLabel vertexLabel,
                              EdgeLabel edgeLabel) {
}
