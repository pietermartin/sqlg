package org.umlg.sqlgraph.process.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.umlg.sqlgraph.structure.SqlEdge;
import org.umlg.sqlgraph.structure.SqlGraph;
import org.umlg.sqlgraph.structure.SqlVertex;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Date: 2014/07/12
 * Time: 5:50 AM
 */
public class SqlVertexStep <E extends Element> extends VertexStep<E> {

    public SqlVertexStep(final SqlGraph g, final Traversal traversal, final Class<E> returnClass,
                           final com.tinkerpop.gremlin.structure.Direction direction,
                           final int branchFactor, final String... labels) {
        super(traversal, returnClass, direction, branchFactor, labels);
        if (Vertex.class.isAssignableFrom(returnClass)) {
            if (direction.equals(Direction.OUT))
                this.setFunction(traverser -> (Iterator) StreamFactory.stream(new SqlVertexVertexIterable<>(g, (SqlVertex) traverser.get(), Direction.OUT, labels)).limit(this.branchFactor).iterator());
            else if (direction.equals(com.tinkerpop.gremlin.structure.Direction.IN))
                this.setFunction(traverser -> (Iterator) StreamFactory.stream(new SqlVertexVertexIterable<>(g, (SqlVertex) traverser.get(), Direction.IN, labels)).limit(this.branchFactor).iterator());
            else
                this.setFunction(traverser -> (Iterator) Stream.concat(StreamFactory.stream(new SqlVertexVertexIterable<>(g, (SqlVertex) traverser.get(), Direction.OUT, labels)),
                        StreamFactory.stream(new SqlVertexVertexIterable<>(g, (SqlVertex) traverser.get(), Direction.IN, labels))).limit(this.branchFactor).iterator());
        } else {
            if (direction.equals(Direction.OUT))
                this.setFunction(traverser -> (Iterator) StreamFactory.stream(new SqlVertexEdgeIterable<>(g, (SqlVertex) traverser.get(), Direction.OUT, labels)).limit(this.branchFactor).iterator());
            else if (direction.equals(com.tinkerpop.gremlin.structure.Direction.IN))
                this.setFunction(traverser -> (Iterator) StreamFactory.stream(new SqlVertexEdgeIterable<>(g, (SqlVertex) traverser.get(), Direction.IN, labels)).limit(this.branchFactor).iterator());
            else
                this.setFunction(traverser -> (Iterator) StreamFactory.stream(new SqlVertexEdgeIterable<>(g, (SqlVertex) traverser.get(), Direction.BOTH, labels)).limit(this.branchFactor).iterator());
        }
    }


    private class SqlVertexVertexIterable<T extends Vertex> implements Iterable<SqlVertex> {
        private final SqlGraph sqlGraph;
        private final SqlVertex sqlVertex;
        private final Direction direction;
        private final String[] labels;

        public SqlVertexVertexIterable(final SqlGraph sqlGraph, final SqlVertex sqlVertex, final Direction direction, final String... labels) {
            this.sqlGraph = sqlGraph;
            this.sqlVertex = sqlVertex;
            this.direction = direction;
            this.labels = labels;
        }

        public Iterator<SqlVertex> iterator() {
            this.sqlGraph.tx().readWrite();
            final Iterator<SqlEdge> itty;
            if (labels.length > 0)
                itty = sqlVertex.getEdges(direction, labels);
            else
                itty = sqlVertex.getEdges(direction);

            return new Iterator<SqlVertex>() {
                public SqlVertex next() {
                    SqlEdge sqlEdge = itty.next();
                    SqlVertex inVertex = sqlEdge.getInVertex();
                    SqlVertex outVertex = sqlEdge.getInVertex();
                    if (sqlVertex.id().equals(inVertex.id())) {
                        return outVertex;
                    } else {
                        return inVertex;
                    }
                }

                public boolean hasNext() {
                    return itty.hasNext();
                }

                public void remove() {
                    itty.remove();
                }
            };
        }
    }

    private class SqlVertexEdgeIterable<T extends Edge> implements Iterable<SqlEdge> {

        private final SqlGraph graph;
        private final SqlVertex sqlVertex;
        private final Direction direction;
        private final String[] labels;

        public SqlVertexEdgeIterable(final SqlGraph graph, final SqlVertex sqlVertex, final Direction direction, final String... labels) {
            this.graph = graph;
            this.sqlVertex = sqlVertex;
            this.direction = direction;
            this.labels = labels;
        }

        public Iterator<SqlEdge> iterator() {
            graph.tx().readWrite();
            final Iterator<SqlEdge> itty;
            if (labels.length > 0)
                itty = sqlVertex.getEdges(direction, labels);
            else
                itty = sqlVertex.getEdges(direction);

            return new Iterator<SqlEdge>() {
                public SqlEdge next() {
                    return itty.next();
                }

                public boolean hasNext() {
                    return itty.hasNext();
                }

                public void remove() {
                    itty.remove();
                }
            };
        }
    }

}
