package org.umlg.sqlg.sql.parse;

import org.umlg.sqlg.util.Preconditions;
import org.umlg.sqlg.services.SqlgPGRoutingFactory;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;

public record PGRoutingConnectedComponentConfig(String name, VertexLabel vertexLabel, EdgeLabel edgeLabel) {

    public PGRoutingConnectedComponentConfig(String name, VertexLabel vertexLabel, EdgeLabel edgeLabel) {
        this.name = name;
        this.vertexLabel = vertexLabel;
        this.edgeLabel = edgeLabel;
        Preconditions.checkState(name.equals(SqlgPGRoutingFactory.pgr_connectedComponents));
    }

}
