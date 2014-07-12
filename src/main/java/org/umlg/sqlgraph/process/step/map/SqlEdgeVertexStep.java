package org.umlg.sqlgraph.process.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlgraph.structure.SqlEdge;
import org.umlg.sqlgraph.structure.SqlGraph;
import org.umlg.sqlgraph.structure.SqlVertex;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2014/07/12
 * Time: 5:49 AM
 */
public class SqlEdgeVertexStep extends EdgeVertexStep {

    public SqlEdgeVertexStep(final Traversal traversal, final SqlGraph graph, final Direction direction) {
        super(traversal, direction);
        this.setFunction(traverser -> {
            SqlEdge sqlEdge = (SqlEdge) traverser.get();
            final List<Vertex> vertices = new ArrayList<>();
            if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH))
                vertices.add(new SqlVertex(sqlEdge.getOutVertexId(), graph));
            if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH))
                vertices.add(new SqlVertex(sqlEdge.getInVertexId(), graph));
            return vertices.iterator();
        });
    }
}
