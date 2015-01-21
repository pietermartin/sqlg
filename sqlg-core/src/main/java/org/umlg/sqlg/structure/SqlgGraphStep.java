package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;

/**
 * Date: 2014/07/12
 * Time: 5:49 AM
 */
public class SqlgGraphStep<E extends Element> extends GraphStep<E> {

    private Logger logger = LoggerFactory.getLogger(SchemaManager.class.getName());
    private SqlgGraph sqlgGraph;
    public final List<HasContainer> hasContainers = new ArrayList<>();

    public SqlgGraphStep(final GraphStep<E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getGraph(SqlgGraph.class), originalGraphStep.getReturnClass(), originalGraphStep.getIds());
        if (originalGraphStep.getLabel().isPresent())
            this.setLabel(originalGraphStep.getLabel().get());
        this.sqlgGraph = originalGraphStep.getGraph(SqlgGraph.class);
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    private Iterator<? extends Edge> edges() {
        Stream<? extends Edge> edgeStream;
        if (this.ids != null && this.ids.length > 0) {
            edgeStream = getEdges();
        //TODO support all Compares
        } else if (this.hasContainers.size() > 1 && this.hasContainers.get(0).key.equals(T.label.getAccessor()) && this.hasContainers.get(1).predicate.equals(Compare.eq)) {
            //Scenario 1, using labeled index via 2 HasContainer
            final HasContainer hasContainer1 = this.hasContainers.get(0);
            final HasContainer hasContainer2 = this.hasContainers.get(1);
            this.hasContainers.remove(hasContainer1);
            this.hasContainers.remove(hasContainer2);
            edgeStream = getEdgesUsingLabeledIndex((String) hasContainer1.value, hasContainer2.key, hasContainer2.value);
        } else if (this.hasContainers.size() > 0 && this.hasContainers.get(0).key.equals(T.label.getAccessor())) {
            HasContainer hasContainer = this.hasContainers.get(0);
            //Scenario 2, using label only for search
            if (hasContainer.predicate == Contains.within || hasContainer.predicate == Contains.without) {
                final List<String> labels = (List<String>) hasContainer.value;
                edgeStream = getEdgesUsingLabel(labels.toArray(new String[labels.size()]));
            } else {
                edgeStream = getEdgesUsingLabel((String) hasContainer.value);
            }
            this.hasContainers.remove(hasContainer);
        } else {
            edgeStream = getEdges();
        }
        return edgeStream.filter(e -> HasContainer.testAll((Edge) e, this.hasContainers)).iterator();
    }

    private Iterator<? extends Vertex> vertices() {
        Stream<? extends Vertex> vertexStream;
        if (this.ids != null && this.ids.length > 0) {
            //if ids are given assume it to be the most significant narrowing of vertices.
            //i.e. has logic will be done in memory.
            vertexStream = getVertices();
        //TODO support all Compares
        } else if (this.hasContainers.size() > 1 && this.hasContainers.get(0).key.equals(T.label.getAccessor()) && this.hasContainers.get(1).predicate.equals(Compare.eq)) {
            //Scenario 1, using labeled index via 2 HasContainers
            final HasContainer hasContainer1 = this.hasContainers.get(0);
            final HasContainer hasContainer2 = this.hasContainers.get(1);
            this.hasContainers.remove(hasContainer1);
            this.hasContainers.remove(hasContainer2);
            vertexStream = getVerticesUsingLabeledIndex((String) hasContainer1.value, hasContainer2.key, hasContainer2.value);
        } else if (this.hasContainers.size() > 0 && this.hasContainers.get(0).key.equals(T.label.getAccessor())) {
            //Scenario 2, using label only for search
            HasContainer hasContainer = this.hasContainers.get(0);
            if (hasContainer.predicate == Contains.within || hasContainer.predicate == Contains.without) {
                final List<String> labels = (List<String>) hasContainer.value;
                vertexStream = getVerticesUsingLabel(labels.toArray(new String[labels.size()]));
            } else {
                vertexStream = getVerticesUsingLabel((String) hasContainer.value);
            }
            this.hasContainers.remove(hasContainer);
        } else {
            vertexStream = getVertices();
        }
        return vertexStream.filter(v -> HasContainer.testAll((Vertex) v, this.hasContainers)).iterator();
    }

    private Stream<? extends Vertex> getVertices() {
        return StreamFactory.stream(this.sqlgGraph.vertexIterator(this.ids));
    }

    private Stream<? extends Edge> getEdges() {
        return StreamFactory.stream(this.sqlgGraph.edgeIterator(this.ids));
    }

    private Stream<? extends Vertex> getVerticesUsingLabel(String... labels) {
        Stream<? extends Vertex> vertices = Stream.empty();
        for (String label : labels) {
            vertices = Stream.concat(vertices, StreamFactory.stream(this._verticesUsingLabel(label)));
        }
        return vertices;
    }

    private Stream<? extends Vertex> getVerticesUsingLabeledIndex(String label, String key, Object value) {
        return StreamFactory.stream(this._verticesUsingLabeledIndex(label, key, value));
    }

    private Stream<? extends Edge> getEdgesUsingLabel(String... labels) {
        Stream<? extends Edge> edges = Stream.empty();
        for (String label : labels) {
            edges = Stream.concat(edges, StreamFactory.stream(this._edgesUsingLabel(label)));
        }
        return edges;
    }

    private Stream<? extends Edge> getEdgesUsingLabeledIndex(String label, String key, Object value) {
        return StreamFactory.stream(this._edgesUsingLabeledIndex(label, key, value));
    }

    private Iterable<? extends Vertex> _verticesUsingLabel(String label) {
        Set<String> schemas;
        SchemaTable schemaTable = SqlgUtil.parseLabelMaybeNoSchema(label);
        if (schemaTable.getSchema() == null) {
            schemas = this.sqlgGraph.getSchemaManager().getSchemasForTable(SchemaManager.VERTEX_PREFIX + schemaTable.getTable());
        } else {
            schemas = new HashSet<>();
            schemas.add(schemaTable.getSchema());
        }
        //check that the schema exist
        List<Vertex> sqlGVertexes = new ArrayList<>();
        //Schemas are null when has('label') searches for a label that does not exist yet.
        if (schemas != null) {
            for (String schema : schemas) {
                //check that the schema exists
                if (this.sqlgGraph.getSchemaManager().tableExist(schema, SchemaManager.VERTEX_PREFIX + schemaTable.getTable())) {
                    StringBuilder sql = new StringBuilder("SELECT * FROM ");
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
                    sql.append(".");
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + schemaTable.getTable()));

                    //JOINS to VERTICES to preload the in and out edges labels
                    sql.append(" a JOIN ");
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()));
                    sql.append(".");
                    sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
                    sql.append(" b ON a.");
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
                    sql.append(" = b.");
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
                    sql.append(" ORDER BY ");
                    sql.append("a.");
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));

                    if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                        sql.append(";");
                    }
                    Connection conn = this.sqlgGraph.tx().getConnection();
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql.toString());
                    }
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                        ResultSet resultSet = preparedStatement.executeQuery();
                        while (resultSet.next()) {
                            long id = resultSet.getLong(1);
                            SqlgVertex sqlGVertex = SqlgVertex.of(this.sqlgGraph, id, schema, schemaTable.getTable());
                            sqlGVertex.loadResultSet(resultSet);

                            this.sqlgGraph.loadVertexAndLabels(sqlGVertexes, resultSet, sqlGVertex);

//                            //Load the in and out labels
//                            Set<SchemaTable> labels = new HashSet<>();
//                            String inCommaSeparatedLabels = resultSet.getString(SchemaManager.VERTEX_IN_LABELS);
//                            this.sqlgGraph.getSchemaManager().convertVertexLabelToSet(labels, inCommaSeparatedLabels);
//                            sqlGVertex.inLabelsForVertex = new HashSet<>();
//                            sqlGVertex.inLabelsForVertex.addAll(labels);
//                            String outCommaSeparatedLabels = resultSet.getString(SchemaManager.VERTEX_OUT_LABELS);
//                            labels.clear();
//                            this.sqlgGraph.getSchemaManager().convertVertexLabelToSet(labels, outCommaSeparatedLabels);
//                            sqlGVertex.outLabelsForVertex = new HashSet<>();
//                            sqlGVertex.outLabelsForVertex.addAll(labels);
//                            sqlGVertexes.add(sqlGVertex);

                            //Load the in and out labels
                            Set<SchemaTable> labels = new HashSet<>();
                            String inCommaSeparatedLabels = resultSet.getString(SchemaManager.VERTEX_IN_LABELS);
                            this.sqlgGraph.getSchemaManager().convertVertexLabelToSet(labels, inCommaSeparatedLabels);
                            sqlGVertex.inLabelsForVertex = new HashSet<>();
                            sqlGVertex.inLabelsForVertex.addAll(labels);
                            String outCommaSeparatedLabels = resultSet.getString(SchemaManager.VERTEX_OUT_LABELS);
                            labels.clear();
                            this.sqlgGraph.getSchemaManager().convertVertexLabelToSet(labels, outCommaSeparatedLabels);
                            sqlGVertex.outLabelsForVertex = new HashSet<>();
                            sqlGVertex.outLabelsForVertex.addAll(labels);
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return sqlGVertexes;
    }

    private Iterable<? extends Vertex> _verticesUsingLabeledIndex(String label, String key, Object value) {
        SchemaTable schemaTable = SqlgUtil.parseLabel(label, this.sqlgGraph.getSqlDialect().getPublicSchema());
        //Check that the table and column exist
        List<Vertex> sqlGVertexes = new ArrayList<>();
        if (this.sqlgGraph.getSchemaManager().tableExist(schemaTable.getSchema(), SchemaManager.VERTEX_PREFIX + schemaTable.getTable()) &&
                this.sqlgGraph.getSchemaManager().columnExists(schemaTable.getSchema(), SchemaManager.VERTEX_PREFIX + schemaTable.getTable(), key)) {
            StringBuilder sql = new StringBuilder("SELECT * FROM ");
            sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
            sql.append(".");
            sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + schemaTable.getTable()));
            //join to VERTICES to get the in and out labels
            sql.append(" a JOIN ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()));
            sql.append(".");
            sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
            sql.append(" b ON a.");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
            sql.append(" = b.");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
            sql.append(" WHERE a.");
            sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(key));
            sql.append(" = ?");
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            Connection conn = this.sqlgGraph.tx().getConnection();
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                Map<String, Object> keyValueMap = new HashMap<>();
                keyValueMap.put(key, value);
                SqlgElement.setKeyValuesAsParameter(this.sqlgGraph, 1, conn, preparedStatement, keyValueMap);
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    long id = resultSet.getLong(1);
                    SqlgVertex sqlGVertex = SqlgVertex.of(this.sqlgGraph, id, schemaTable.getSchema(), schemaTable.getTable());
                    sqlGVertex.loadResultSet(resultSet);

                    this.sqlgGraph.loadVertexAndLabels(sqlGVertexes, resultSet, sqlGVertex);
//                    //Load the in and out labels
//                    Set<SchemaTable> labels = new HashSet<>();
//                    String inCommaSeparatedLabels = resultSet.getString(SchemaManager.VERTEX_IN_LABELS);
//                    this.sqlgGraph.getSchemaManager().convertVertexLabelToSet(labels, inCommaSeparatedLabels);
//                    sqlGVertex.inLabelsForVertex = new HashSet<>();
//                    sqlGVertex.inLabelsForVertex.addAll(labels);
//                    String outCommaSeparatedLabels = resultSet.getString(SchemaManager.VERTEX_OUT_LABELS);
//                    labels.clear();
//                    this.sqlgGraph.getSchemaManager().convertVertexLabelToSet(labels, outCommaSeparatedLabels);
//                    sqlGVertex.outLabelsForVertex = new HashSet<>();
//                    sqlGVertex.outLabelsForVertex.addAll(labels);
//                    sqlGVertexes.add(sqlGVertex);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return sqlGVertexes;
    }

    private Iterable<? extends Edge> _edgesUsingLabel(String label) {
        Set<String> schemas;
        SchemaTable schemaTable = SqlgUtil.parseLabelMaybeNoSchema(label);
        if (schemaTable.getSchema() == null) {
            schemas = this.sqlgGraph.getSchemaManager().getSchemasForTable(SchemaManager.EDGE_PREFIX + schemaTable.getTable());
        } else {
            schemas = new HashSet<>();
            schemas.add(schemaTable.getSchema());
        }
        List<SqlgEdge> sqlGEdges = new ArrayList<>();
        for (String schema : schemas) {
            StringBuilder sql = new StringBuilder("SELECT * FROM ");
            sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + schemaTable.getTable()));
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            Connection conn = this.sqlgGraph.tx().getConnection();
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    long id = resultSet.getLong(1);
                    SqlgEdge sqlGEdge = new SqlgEdge(this.sqlgGraph, id, schema, schemaTable.getTable());
                    //TODO properties needs to be cached
                    sqlGEdge.loadResultSet(resultSet);
                    sqlGEdges.add(sqlGEdge);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return sqlGEdges;
    }

    private Iterable<? extends Edge> _edgesUsingLabeledIndex(String label, String key, Object value) {
        List<SqlgEdge> sqlGEdges = new ArrayList<>();
        SchemaTable schemaTable = SqlgUtil.parseLabel(label, this.sqlgGraph.getSqlDialect().getPublicSchema());
        if (this.sqlgGraph.getSchemaManager().tableExist(schemaTable.getSchema(), SchemaManager.EDGE_PREFIX + schemaTable.getTable()) &&
                this.sqlgGraph.getSchemaManager().columnExists(schemaTable.getSchema(), SchemaManager.EDGE_PREFIX + schemaTable.getTable(), key)) {
            StringBuilder sql = new StringBuilder("SELECT * FROM ");
            sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
            sql.append(".");
            sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + schemaTable.getTable()));
            sql.append(" a WHERE a.");
            sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(key));
            sql.append(" = ?");
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            Connection conn = this.sqlgGraph.tx().getConnection();
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                Map<String, Object> keyValueMap = new HashMap<>();
                keyValueMap.put(key, value);
                SqlgElement.setKeyValuesAsParameter(this.sqlgGraph, 1, conn, preparedStatement, keyValueMap);
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    long id = resultSet.getLong(1);
                    SqlgEdge sqlGEdge = new SqlgEdge(this.sqlgGraph, id, schemaTable.getSchema(), schemaTable.getTable());
                    //TODO properties needs to be cached
                    sqlGEdge.loadResultSet(resultSet);
                    sqlGEdges.add(sqlGEdge);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return sqlGEdges;
    }

}
