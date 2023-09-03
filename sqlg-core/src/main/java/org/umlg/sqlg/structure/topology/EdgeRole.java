package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.TopologyInf;

import java.util.Optional;

/**
 * Represent a pair VertexLabel and EdgeLabel
 *
 * @author jpmoresmau
 */
public class EdgeRole implements TopologyInf {

    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeRole.class);

    /**
     * the vertex label
     */
    private final VertexLabel vertexLabel;
    /**
     * the edge label
     */
    private final EdgeLabel edgeLabel;

    /**
     * the direction of the edge for the vertex label
     */
    private final Direction direction;

    /**
     * are we committed or still in a transaction?
     */
    private final boolean committed;

    private final Multiplicity multiplicity;

    EdgeRole(VertexLabel vertexLabel, EdgeLabel edgeLabel, Direction direction, boolean committed, Multiplicity multiplicity) {
        super();
        this.vertexLabel = vertexLabel;
        this.edgeLabel = edgeLabel;
        this.direction = direction;
        this.committed = committed;
        this.multiplicity = multiplicity;
    }

    public VertexLabel getVertexLabel() {
        return vertexLabel;
    }

    public EdgeLabel getEdgeLabel() {
        return edgeLabel;
    }

    public Multiplicity getMultiplicity() {
        return multiplicity;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((direction == null) ? 0 : direction.hashCode());
        result = prime * result + ((multiplicity == null) ? 0 : multiplicity.hashCode());
        result = prime * result + ((edgeLabel == null) ? 0 : edgeLabel.getLabel().hashCode());
        result = prime * result + ((vertexLabel == null) ? 0 : vertexLabel.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EdgeRole other = (EdgeRole) obj;
        if (direction != other.direction)
            return false;
        if (!multiplicity.equals(other.multiplicity)) {
            return false;
        }
        if (edgeLabel == null) {
            if (other.edgeLabel != null)
                return false;
        } else if (!edgeLabel.equals(other.edgeLabel))
            return false;
        if (vertexLabel == null) {
            return other.vertexLabel == null;
        } else return vertexLabel.equals(other.vertexLabel);
    }

    public Direction getDirection() {
        return direction;
    }

    @Override
    public boolean isCommitted() {
        return committed;
    }

    @Override
    public void remove(boolean preserveData) {
        getVertexLabel().removeEdgeRole(this, true, preserveData);
    }

    void removeViaVertexLabelRemove(boolean preserveData) {
        getVertexLabel().removeEdgeRole(this, false, preserveData);
    }

    @Override
    public String getName() {
        String direction = switch (this.direction) {
            case BOTH -> "<->";
            case IN -> "<-";
            case OUT -> "->";
        };
        return vertexLabel.getName() + direction + edgeLabel.getName();
    }
    
    Optional<JsonNode> toNotifyJson() {
        ObjectNode edgeRoleNode = Topology.OBJECT_MAPPER.createObjectNode();
        edgeRoleNode.put("direction", direction.name());
        edgeRoleNode.set("multiplicity", multiplicity.toNotifyJson());
        Optional<JsonNode> edgeLabel = getEdgeLabel().toNotifyJson();
        if (edgeLabel.isPresent()) {
            edgeRoleNode.set("edgeLabel", edgeLabel.get());
            return Optional.of(edgeRoleNode);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public String toString() {
        return getName();
    }
}
