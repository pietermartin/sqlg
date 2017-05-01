package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * Represent a pair VertexLabel and EdgeLabel
 * @author jpmoresmau
 *
 */
public class EdgeRole implements TopologyInf {
	private VertexLabel vertexLabel;
	private EdgeLabel edgeLabel;
	
	private Direction direction;
	
	private boolean committed;
	
	EdgeRole(VertexLabel vertexLabel, EdgeLabel edgeLabel, Direction direction, boolean committed) {
		super();
		this.vertexLabel = vertexLabel;
		this.edgeLabel = edgeLabel;
		this.direction = direction;
		this.committed = committed;
	}

	public VertexLabel getVertexLabel() {
		return vertexLabel;
	}

	public EdgeLabel getEdgeLabel() {
		return edgeLabel;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((direction == null) ? 0 : direction.hashCode());
		result = prime * result + ((edgeLabel == null) ? 0 : edgeLabel.hashCode());
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
		if (edgeLabel == null) {
			if (other.edgeLabel != null)
				return false;
		} else if (!edgeLabel.equals(other.edgeLabel))
			return false;
		if (vertexLabel == null) {
			if (other.vertexLabel != null)
				return false;
		} else if (!vertexLabel.equals(other.vertexLabel))
			return false;
		return true;
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
		getVertexLabel().removeEdgeRole(this, preserveData);
	}
	
	@Override
	public String getName() {
		String dir="";
		switch (direction){
			case BOTH: dir="<->";
				break;
			case IN: dir="<-";
				break;
			case OUT: dir="->";
				break;
		}
		return vertexLabel.getName()+dir+edgeLabel.getName();
	}
	
	@Override
	public String toString() {
		return getName();
	}
}
