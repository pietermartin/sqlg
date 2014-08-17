package org.umlg.sqlg.process.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.GraphStep;
import com.tinkerpop.gremlin.process.util.TraverserIterator;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.umlg.sqlg.structure.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Date: 2014/07/12
 * Time: 5:49 AM
 */
public class SqlGGraphStep<E extends Element> extends GraphStep<E> {

    private SqlG sqlG;
    public final List<HasContainer> hasContainers = new ArrayList<>();

    public SqlGGraphStep(final Traversal traversal, final Class<E> returnClass, final SqlG sqlG) {
        super(traversal, returnClass);
        this.sqlG = sqlG;
    }

    @Override
    public void generateTraverserIterator(boolean trackPaths) {
        this.sqlG.tx().readWrite();
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
        Stream<? extends Edge> edgeStream = getEdges();
        return edgeStream.filter(e -> HasContainer.testAll((Edge) e, this.hasContainers)).iterator();
    }

    private Iterator<? extends Vertex> vertices() {
//        return __vertices();
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
        List<SqlgVertex> sqlGVertexes = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlG.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                long id = resultSet.getLong(1);
                String schema = resultSet.getString(2);
                String table = resultSet.getString(3);
                SqlgVertex sqlGVertex = new SqlgVertex(this.sqlG, id, schema, table);
                sqlGVertexes.add(sqlGVertex);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Collections.reverse(sqlGVertexes);
        return sqlGVertexes;
    }

    private Iterator<? extends Vertex> __vertices() {
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlG.tx().getConnection();
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(sql.toString());
            ResultSet resultSet = preparedStatement.executeQuery();
            return new ResultSetIterator(this.sqlG, resultSet);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Iterable<? extends Edge> _edges() {
        List<SqlgEdge> sqlGEdges = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGES));
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlG.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                long id = resultSet.getLong(1);
                String schema = resultSet.getString(2);
                String table = resultSet.getString(3);
                SchemaTable schemaTablePair = SchemaTable.of(schema, table);
                SqlgEdge sqlGEdge = new SqlgEdge(this.sqlG, id, schemaTablePair.getSchema(), schemaTablePair.getTable());
                sqlGEdges.add(sqlGEdge);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return sqlGEdges;
    }

}
