package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * Date: 2014/07/12
 * Time: 5:49 AM
 */
public class SqlgGraphStep<E extends Element> /* extends GraphStep<E> */ {

//    private Logger logger = LoggerFactory.getLogger(SqlgGraphStep.class.getName());
//    public final List<HasContainer> hasContainers = new ArrayList<>();
//
//    public SqlgGraphStep(final GraphStep<E> originalGraphStep) {
//        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.getIds());
//        if (!originalGraphStep.getLabels().isEmpty()) {
//            originalGraphStep.getLabels().forEach(this::addLabel);
//        }
//        if ((this.ids.length == 0 || !(this.ids[0] instanceof Element)))
//            this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
//    }
//
//    private Iterator<? extends Edge> edges() {
//        Stream<? extends Edge> edgeStream;
//        if (this.ids != null && this.ids.length > 0) {
//            edgeStream = getEdges();
//            //TODO support all Compares
//        } else if (this.hasContainers.size() > 1 && this.hasContainers.get(0).getKey().equals(T.label.getAccessor()) && this.hasContainers.get(1).getBiPredicate().equals(Compare.eq)) {
//            //Scenario 1, using labeled index via 2 HasContainer
//            final HasContainer hasContainer1 = this.hasContainers.get(0);
//            final HasContainer hasContainer2 = this.hasContainers.get(1);
//            this.hasContainers.remove(hasContainer1);
//            this.hasContainers.remove(hasContainer2);
//            edgeStream = getEdgesUsingLabeledIndex((String) hasContainer1.getValue(), hasContainer2.getKey(), hasContainer2.getValue());
//        } else if (this.hasContainers.size() > 0 && this.hasContainers.get(0).getKey().equals(T.label.getAccessor())) {
//            HasContainer hasContainer = this.hasContainers.get(0);
//            //Scenario 2, using label only for search
//            if (hasContainer.getBiPredicate() == Contains.within || hasContainer.getBiPredicate() == Contains.without) {
//                final List<String> labels = (List<String>) hasContainer.getValue();
//                edgeStream = getEdgesUsingLabel(labels.toArray(new String[labels.size()]));
//            } else {
//                edgeStream = getEdgesUsingLabel((String) hasContainer.getValue());
//            }
//            this.hasContainers.remove(hasContainer);
//        } else {
//            edgeStream = getEdges();
//        }
//        return edgeStream.filter(e -> HasContainer.testAll((Edge) e, this.hasContainers)).iterator();
//    }
//
//    private Iterator<? extends Vertex> vertices() {
//        Stream<? extends Vertex> vertexStream;
//        if (this.ids != null && this.ids.length > 0) {
//            //if ids are given assume it to be the most significant narrowing of vertices.
//            //i.e. has logic will be done in memory.
//            vertexStream = getVertices();
//            //TODO support all Compares
//        } else if (this.hasContainers.size() > 1 && this.hasContainers.get(0).getKey().equals(T.label.getAccessor()) && this.hasContainers.get(1).getBiPredicate().equals(Compare.eq)) {
//            //Scenario 1, using labeled index via 2 HasContainers
//            final HasContainer hasContainer1 = this.hasContainers.get(0);
//            final HasContainer hasContainer2 = this.hasContainers.get(1);
//            this.hasContainers.remove(hasContainer1);
//            this.hasContainers.remove(hasContainer2);
//            vertexStream = getVerticesUsingLabeledIndex((String) hasContainer1.getValue(), hasContainer2.getKey(), hasContainer2.getValue());
//        } else if (this.hasContainers.size() > 0 && this.hasContainers.get(0).getKey().equals(T.label.getAccessor())) {
//            //Scenario 2, using label only for search
//            HasContainer hasContainer = this.hasContainers.get(0);
//            if (hasContainer.getBiPredicate() == Contains.within || hasContainer.getBiPredicate() == Contains.without) {
//                final List<String> labels = (List<String>) hasContainer.getValue();
//                vertexStream = getVerticesUsingLabel(labels.toArray(new String[labels.size()]));
//            } else {
//                vertexStream = getVerticesUsingLabel((String) hasContainer.getValue());
//            }
//            this.hasContainers.remove(hasContainer);
//        } else {
//            vertexStream = getVertices();
//        }
//        return vertexStream.filter(v -> HasContainer.testAll((Vertex) v, this.hasContainers)).iterator();
//    }
//
//    private Stream<? extends Vertex> getVertices() {
//        SqlgGraph sqlgGraph = (SqlgGraph) this.getTraversal().getGraph().get();
//        return StreamFactory.stream(sqlgGraph.vertices(this.ids));
//    }
//
//    private Stream<? extends Edge> getEdges() {
//        SqlgGraph sqlgGraph = (SqlgGraph) this.getTraversal().getGraph().get();
//        return StreamFactory.stream(sqlgGraph.edges(this.ids));
//    }
//
//    private Stream<? extends Vertex> getVerticesUsingLabel(String... labels) {
//        Stream<? extends Vertex> vertices = Stream.empty();
//        for (String label : labels) {
//            Iterable<? extends Vertex> iterable = this._verticesUsingLabel(label);
//            Stream<? extends Vertex> targetStream = StreamSupport.stream(iterable.spliterator(), false);
//            vertices = Stream.concat(vertices, targetStream);
//        }
//        return vertices;
//    }
//
//    private Stream<? extends Vertex> getVerticesUsingLabeledIndex(String label, String key, Object value) {
//        return StreamSupport.stream(this._verticesUsingLabeledIndex(label, key, value).spliterator(), false);
//    }
//
//    private Stream<? extends Edge> getEdgesUsingLabel(String... labels) {
//        Stream<? extends Edge> edges = Stream.empty();
//        for (String label : labels) {
//            Iterable<? extends Edge> iterable = this._edgesUsingLabel(label);
//            Stream<? extends Edge> targetStream = StreamSupport.stream(iterable.spliterator(), false);
//            edges = Stream.concat(edges, targetStream);
//        }
//        return edges;
//    }
//
//    private Stream<? extends Edge> getEdgesUsingLabeledIndex(String label, String key, Object value) {
//        return StreamSupport.stream(this._edgesUsingLabeledIndex(label, key, value).spliterator(), false);
//    }
//
//    private Iterable<? extends Vertex> _verticesUsingLabel(String label) {
//        SqlgGraph sqlgGraph = (SqlgGraph) this.getTraversal().getGraph().get();
//        Set<String> schemas;
//        SchemaTable schemaTable = SqlgUtil.parseLabelMaybeNoSchema(label);
//        if (schemaTable.getSchema() == null) {
//            schemas = sqlgGraph.getSchemaManager().getSchemasForTable(SchemaManager.VERTEX_PREFIX + schemaTable.getTable());
//        } else {
//            schemas = new HashSet<>();
//            schemas.add(schemaTable.getSchema());
//        }
//        //check that the schema exist
//        List<Vertex> sqlGVertexes = new ArrayList<>();
//        //Schemas are null when has('label') searches for a label that does not exist yet.
//        if (schemas != null) {
//            for (String schema : schemas) {
//                //check that the schema exists
//                if (sqlgGraph.getSchemaManager().tableExist(schema, SchemaManager.VERTEX_PREFIX + schemaTable.getTable())) {
//                    StringBuilder sql = new StringBuilder("SELECT * FROM ");
//                    sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
//                    sql.append(".");
//                    sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + schemaTable.getTable()));
//                    sql.append(" a ");
//                    sql.append(" ORDER BY ");
//                    sql.append("a.");
//                    sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
//
//                    if (sqlgGraph.getSqlDialect().needsSemicolon()) {
//                        sql.append(";");
//                    }
//                    Connection conn = sqlgGraph.tx().getConnection();
//                    if (logger.isDebugEnabled()) {
//                        logger.debug(sql.toString());
//                    }
//                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
//                        ResultSet resultSet = preparedStatement.executeQuery();
//                        while (resultSet.next()) {
//                            long id = resultSet.getLong(1);
//                            SqlgVertex sqlGVertex = SqlgVertex.of(sqlgGraph, id, schema, schemaTable.getTable());
//                            sqlGVertex.loadResultSet(resultSet);
//                            sqlGVertexes.add(sqlGVertex);
//                        }
//                    } catch (SQLException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//            }
//        }
//        return sqlGVertexes;
//    }
//
//    private Iterable<? extends Vertex> _verticesUsingLabeledIndex(String label, String key, Object value) {
//        SqlgGraph sqlgGraph = (SqlgGraph) this.getTraversal().getGraph().get();
//        SchemaTable schemaTable = SchemaTable.from(sqlgGraph, label, sqlgGraph.getSqlDialect().getPublicSchema());
//        //Check that the table and column exist
//        List<Vertex> sqlGVertexes = new ArrayList<>();
//        if (sqlgGraph.getSchemaManager().tableExist(schemaTable.getSchema(), SchemaManager.VERTEX_PREFIX + schemaTable.getTable()) &&
//                sqlgGraph.getSchemaManager().columnExists(schemaTable.getSchema(), SchemaManager.VERTEX_PREFIX + schemaTable.getTable(), key)) {
//            StringBuilder sql = new StringBuilder("SELECT * FROM ");
//            sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
//            sql.append(".");
//            sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + schemaTable.getTable()));
//            sql.append(" a ");
//            sql.append(" WHERE a.");
//            sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(key));
//            sql.append(" = ?");
//            if (sqlgGraph.getSqlDialect().needsSemicolon()) {
//                sql.append(";");
//            }
//            Connection conn = sqlgGraph.tx().getConnection();
//            if (logger.isDebugEnabled()) {
//                logger.debug(sql.toString());
//            }
//            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
//                Map<String, Object> keyValueMap = new HashMap<>();
//                keyValueMap.put(key, value);
//                SqlgElement.setKeyValuesAsParameter(sqlgGraph, 1, conn, preparedStatement, keyValueMap);
//                ResultSet resultSet = preparedStatement.executeQuery();
//                while (resultSet.next()) {
//                    long id = resultSet.getLong(1);
//                    SqlgVertex sqlGVertex = SqlgVertex.of(sqlgGraph, id, schemaTable.getSchema(), schemaTable.getTable());
//                    sqlGVertex.loadResultSet(resultSet);
//                    sqlGVertexes.add(sqlGVertex);
//                }
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        return sqlGVertexes;
//    }
//
//    private Iterable<? extends Edge> _edgesUsingLabel(String label) {
//        SqlgGraph sqlgGraph = (SqlgGraph) this.getTraversal().getGraph().get();
//        Set<String> schemas;
//        SchemaTable schemaTable = SqlgUtil.parseLabelMaybeNoSchema(label);
//        if (schemaTable.getSchema() == null) {
//            schemas = sqlgGraph.getSchemaManager().getSchemasForTable(SchemaManager.EDGE_PREFIX + schemaTable.getTable());
//        } else {
//            schemas = new HashSet<>();
//            schemas.add(schemaTable.getSchema());
//        }
//        List<SqlgEdge> sqlGEdges = new ArrayList<>();
//        for (String schema : schemas) {
//            StringBuilder sql = new StringBuilder("SELECT * FROM ");
//            sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(schema));
//            sql.append(".");
//            sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + schemaTable.getTable()));
//            if (sqlgGraph.getSqlDialect().needsSemicolon()) {
//                sql.append(";");
//            }
//            Connection conn = sqlgGraph.tx().getConnection();
//            if (logger.isDebugEnabled()) {
//                logger.debug(sql.toString());
//            }
//            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
//                ResultSet resultSet = preparedStatement.executeQuery();
//                while (resultSet.next()) {
//                    long id = resultSet.getLong(1);
//                    SqlgEdge sqlGEdge = new SqlgEdge(sqlgGraph, id, schema, schemaTable.getTable());
//                    //TODO properties needs to be cached
//                    sqlGEdge.loadResultSet(resultSet);
//                    sqlGEdges.add(sqlGEdge);
//                }
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        return sqlGEdges;
//    }
//
//    private Iterable<? extends Edge> _edgesUsingLabeledIndex(String label, String key, Object value) {
//        SqlgGraph sqlgGraph = (SqlgGraph) this.getTraversal().getGraph().get();
//        List<SqlgEdge> sqlGEdges = new ArrayList<>();
//        SchemaTable schemaTable = SchemaTable.from(sqlgGraph, label, sqlgGraph.getSqlDialect().getPublicSchema());
//        if (sqlgGraph.getSchemaManager().tableExist(schemaTable.getSchema(), SchemaManager.EDGE_PREFIX + schemaTable.getTable()) &&
//                sqlgGraph.getSchemaManager().columnExists(schemaTable.getSchema(), SchemaManager.EDGE_PREFIX + schemaTable.getTable(), key)) {
//            StringBuilder sql = new StringBuilder("SELECT * FROM ");
//            sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
//            sql.append(".");
//            sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + schemaTable.getTable()));
//            sql.append(" a WHERE a.");
//            sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(key));
//            sql.append(" = ?");
//            if (sqlgGraph.getSqlDialect().needsSemicolon()) {
//                sql.append(";");
//            }
//            Connection conn = sqlgGraph.tx().getConnection();
//            if (logger.isDebugEnabled()) {
//                logger.debug(sql.toString());
//            }
//            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
//                Map<String, Object> keyValueMap = new HashMap<>();
//                keyValueMap.put(key, value);
//                SqlgElement.setKeyValuesAsParameter(sqlgGraph, 1, conn, preparedStatement, keyValueMap);
//                ResultSet resultSet = preparedStatement.executeQuery();
//                while (resultSet.next()) {
//                    long id = resultSet.getLong(1);
//                    SqlgEdge sqlGEdge = new SqlgEdge(sqlgGraph, id, schemaTable.getSchema(), schemaTable.getTable());
//                    //TODO properties needs to be cached
//                    sqlGEdge.loadResultSet(resultSet);
//                    sqlGEdges.add(sqlGEdge);
//                }
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        return sqlGEdges;
//    }

}
