package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import org.umlg.sqlg.services.SqlgPGRoutingFactory;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;

import java.util.List;

public record PGRoutingDrivingDistanceConfig(String name, List<Long> start_vids, boolean directed, Long distance,
                                             VertexLabel vertexLabel,
                                             EdgeLabel edgeLabel) {

    public PGRoutingDrivingDistanceConfig(String name, List<Long> start_vids, boolean directed, Long distance, VertexLabel vertexLabel, EdgeLabel edgeLabel) {
        this.name = name;
        this.start_vids = start_vids;
        this.directed = directed;
        this.distance = distance;
        this.vertexLabel = vertexLabel;
        this.edgeLabel = edgeLabel;
        Preconditions.checkState(name.equals(SqlgPGRoutingFactory.pgr_drivingDistance));
        Preconditions.checkState(!start_vids.isEmpty());
        Preconditions.checkState(distance != null);
    }

    public String toStartVidsString() {
        if (start_vids().size() == 1) {
            return start_vids().get(0).toString();
        } else {
            return "ARRAY[" + String.join(",", start_vids().stream().map(Object::toString).toArray(String[]::new)) + "]";
        }
    }

}
