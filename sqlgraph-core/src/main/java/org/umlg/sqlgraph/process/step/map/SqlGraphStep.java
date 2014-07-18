package org.umlg.sqlgraph.process.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.GraphStep;
import com.tinkerpop.gremlin.process.util.TraverserIterator;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.umlg.sqlgraph.structure.SchemaManager;
import org.umlg.sqlgraph.structure.SqlEdge;
import org.umlg.sqlgraph.structure.SqlGraph;
import org.umlg.sqlgraph.structure.SqlVertex;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Date: 2014/07/12
 * Time: 5:49 AM
 */
public class SqlGraphStep<E extends Element> extends GraphStep<E> {

    private SqlGraph sqlGraph;
    public final List<HasContainer> hasContainers = new ArrayList<>();

    public SqlGraphStep(final Traversal traversal, final Class<E> returnClass, final SqlGraph sqlGraph) {
        super(traversal, returnClass);
        this.sqlGraph = sqlGraph;
    }

    @Override
    public void generateTraverserIterator(boolean trackPaths) {
        this.sqlGraph.tx().readWrite();
        this.starts.clear();
        if (trackPaths)
            this.starts.add(new TraverserIterator(this, Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
        else
            this.starts.add(new TraverserIterator(Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));

    }

    @Override
    public void clear() {
        this.starts.clear();
    }

    private Iterator<? extends Edge> edges() {
//        final HasContainer indexedContainer = getIndexKey(Edge.class);
//        Stream<Edge> edgeStream = (indexedContainer != null) ?
//                getEdgesUsingIndex(indexedContainer) :
//                getEdges();
        Stream<? extends Edge> edgeStream = getEdges();
        return edgeStream.filter(e -> HasContainer.testAll((Edge) e, this.hasContainers)).iterator();
    }

    private Iterator<? extends Vertex> vertices() {
//        final HasContainer indexedContainer = getIndexKey(Vertex.class);
//        Stream<Vertex> vertexStream = (indexedContainer != null) ?
//                getVerticesUsingIndex(indexedContainer) :
//                getVertices();
        Stream<? extends Vertex> vertexStream = getVertices();
        return vertexStream.filter(v -> HasContainer.testAll((Vertex) v, this.hasContainers)).iterator();
    }

    private Stream<? extends Vertex> getVertices() {
        return StreamFactory.stream(this._vertices());
    }

    private Stream<? extends Edge> getEdges() {
        return StreamFactory.stream(this._edges());
    }

    private Iterable<? extends Vertex> _vertices() {
        List<SqlVertex> sqlVertexes = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
        if (this.sqlGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                long id = resultSet.getLong(1);
                String tableName = resultSet.getString(2);
                SqlVertex sqlVertex = new SqlVertex(this.sqlGraph, id, tableName);
                sqlVertexes.add(sqlVertex);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return sqlVertexes;
    }

    private Iterable<? extends Edge> _edges() {
        List<SqlEdge> sqlEdges = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGES));
        if (this.sqlGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                long id = resultSet.getLong(1);
                String tableName = resultSet.getString(2);
                SqlEdge sqlEdge = new SqlEdge(this.sqlGraph, id, tableName);
                sqlEdges.add(sqlEdge);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return sqlEdges;
    }

}
