package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Date: 2015/01/03
 * Time: 1:06 PM
 */
public class GremlinParser {

    private SqlgGraph sqlgGraph;
    private SchemaManager schemaManager;

    public GremlinParser(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
        this.schemaManager = sqlgGraph.getSchemaManager();
    }

    /**
     * This will create SqlgVertex from the resultSet.
     * Note that the nature of inner join queries on multiple tables creates duplicates.
     * This method filters out duplicates.
     *
     * @param resultSet
     * @param previousSchemaTables
     * @param vertices
     * @throws SQLException
     */
    public void loadVertices(ResultSet resultSet, Set<SchemaTable> previousSchemaTables, List<Vertex> vertices) throws SQLException {
        for (SchemaTable previousSchemaTable : previousSchemaTables) {
            List<Object> keyValues = new ArrayList<>();
            String idProperty = previousSchemaTable.getSchema() + "." + previousSchemaTable.getTable() + "." + SchemaManager.ID;
            Long id = resultSet.getLong(idProperty);
            Map<String, PropertyType> propertyTypeMap = this.sqlgGraph.getSchemaManager().getLocalTables().get(previousSchemaTable.toString());
            for (String propertyName : propertyTypeMap.keySet()) {
                String property = previousSchemaTable.getSchema() + "." + previousSchemaTable.getTable() + "." + propertyName;
                keyValues.add(property);
                keyValues.add(resultSet.getObject(property));
            }
            Map<String, Object> keyValueMap = SqlgUtil.transformToInsertValues(keyValues.toArray());
            SqlgVertex sqlGVertex = SqlgVertex.of(this.sqlgGraph, id, previousSchemaTable.getSchema(), previousSchemaTable.getTable());
            sqlGVertex.properties.clear();
            sqlGVertex.properties.putAll(keyValueMap);
            vertices.add(sqlGVertex);
        }
    }

    public String constructFromClause(SqlgVertex sqlgVertex, Set<SchemaTable> previousSchemaTables) {
        String sql = "SELECT ";
        int count = 1;
        for (SchemaTable previousSchemaTable : previousSchemaTables) {
            String finalSchemaTableName = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(previousSchemaTable.getSchema());
            finalSchemaTableName += ".";
            finalSchemaTableName += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(previousSchemaTable.getTable());
            sql += finalSchemaTableName + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
            sql += " AS \"" + previousSchemaTable.getSchema() + "." + previousSchemaTable.getTable() + "." + SchemaManager.ID + "\"";
            Map<String, PropertyType> propertyTypeMap = this.sqlgGraph.getSchemaManager().getLocalTables().get(previousSchemaTable.toString());
            if (propertyTypeMap.size() > 0) {
                sql += ", ";
            }
            int propertyCount = 1;
            for (String propertyName : propertyTypeMap.keySet()) {
                sql += finalSchemaTableName + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(propertyName);
                sql += " AS \"" + previousSchemaTable.getSchema() + "." + previousSchemaTable.getTable() + "." + propertyName + "\"";
                if (propertyCount < propertyTypeMap.size()) {
                    sql += ",";
                }
            }
            if (count++ < previousSchemaTables.size()) {
                sql += ", ";
            }
        }
        sql += " FROM ";
        sql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(sqlgVertex.schema);
        sql += ".";
        sql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + sqlgVertex.table);
        return sql;
    }

    /**
     * Constructs the label paths from the given schemaTable to the leaf vertex labels for the gremlin query.
     * For each path Sqlg will execute a sql query. The union of the queries is the result the gremlin query.
     * The vertex labels can be calculated from the steps.
     *
     * @param schemaTable
     * @param replacedSteps
     * @return a List of paths. Each path is itself a list of SchemaTables.
     */
    public SchemaTableTree calculateDistinctPathsToLeafVertices(SchemaTable schemaTable, List<Step> replacedSteps) {
        Set<SchemaTableTree> schemaTableTrees = new HashSet<>();
        SchemaTableTree rootSchemaTableTree = new SchemaTableTree(schemaTable, 0);
        schemaTableTrees.add(rootSchemaTableTree);
        int depth = 1;
        for (Step vertexStep : replacedSteps) {
            schemaTableTrees = calculatePathForVertexStep(schemaTableTrees, (VertexStep) vertexStep, depth++);
        }
        System.out.println(rootSchemaTableTree.toString());
        rootSchemaTableTree.postParse(replacedSteps.size());
        System.out.println(rootSchemaTableTree.toString());
        return rootSchemaTableTree;
    }

    private Set<SchemaTableTree> calculatePathForVertexStep(Set<SchemaTableTree> schemaTableTrees, VertexStep vertexStep, int depth) {
        Set<SchemaTableTree> result = new HashSet<>();
        for (SchemaTableTree schemaTableTree : schemaTableTrees) {
            String[] edgeLabels = vertexStep.getEdgeLabels();
            Direction direction = vertexStep.getDirection();
            Class elementClass = vertexStep.getReturnClass();

            Pair<Set<SchemaTable>, Set<SchemaTable>> labels = this.schemaManager.getLocalTableLabels().get(schemaTableTree.getSchemaTable());
            Set<SchemaTable> inLabels = labels.getLeft();
            Set<SchemaTable> outLabels = labels.getRight();
            Set<SchemaTable> inLabelsToTraversers = new HashSet<>();
            Set<SchemaTable> outLabelsToTraversers = new HashSet<>();
            switch (direction) {
                case IN:
                    inLabelsToTraversers = edgeLabels.length > 0 ? filter(inLabels, edgeLabels) : inLabels;
                    break;
                case OUT:
                    outLabelsToTraversers = edgeLabels.length > 0 ? filter(outLabels, edgeLabels) : outLabels;
                    break;
                case BOTH:
                    inLabelsToTraversers = edgeLabels.length > 0 ? filter(inLabels, edgeLabels) : inLabels;
                    outLabelsToTraversers = edgeLabels.length > 0 ? filter(outLabels, edgeLabels) : outLabels;
                    break;
                default:
                    throw new IllegalStateException("Unknown direction " + direction.name());
            }
            //Each labelToTravers more than the first one forms a new distinct path
            for (SchemaTable inLabelsToTravers : inLabelsToTraversers) {
                SchemaTableTree schemaTableTreeChild = schemaTableTree.addChild(inLabelsToTravers, depth);
                result.addAll(calculatePathFromEdgeToVertex(schemaTableTreeChild, inLabelsToTravers, Direction.IN, depth));

            }
            for (SchemaTable outLabelsToTravers : outLabelsToTraversers) {
                SchemaTableTree schemaTableTreeChild = schemaTableTree.addChild(outLabelsToTravers, depth);
                result.addAll(calculatePathFromEdgeToVertex(schemaTableTreeChild, outLabelsToTravers, Direction.OUT, depth));
            }
        }
        return result;
    }

    private Set<SchemaTableTree> calculatePathFromEdgeToVertex(SchemaTableTree schemaTableTree, SchemaTable labelToTravers, Direction direction, int depth) {
        Set<SchemaTableTree> result = new HashSet<>();
        Map<String, Set<String>> edgeForeignKeys = this.schemaManager.getEdgeForeignKeys();
        //join from the edge table to the incoming vertex table
        Set<String> foreignKeys = edgeForeignKeys.get(labelToTravers.toString());
        //Every foreignKey for the given direction must be joined on
        for (String foreignKey : foreignKeys) {
            String foreignKeySchema = foreignKey.split("\\.")[0];
            String foreignKeyTable = foreignKey.split("\\.")[1];
            if (direction == Direction.IN && foreignKey.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                SchemaTableTree schemaTableTree1 = schemaTableTree.addChild(
                        SchemaTable.of(foreignKeySchema, SchemaManager.VERTEX_PREFIX + foreignKeyTable.replace(SchemaManager.OUT_VERTEX_COLUMN_END, "")),
                        depth);
                result.add(schemaTableTree1);
            } else if (direction == Direction.OUT && foreignKey.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
                SchemaTableTree schemaTableTree1 = schemaTableTree.addChild(
                        SchemaTable.of(foreignKeySchema, SchemaManager.VERTEX_PREFIX + foreignKeyTable.replace(SchemaManager.IN_VERTEX_COLUMN_END, "")),
                        depth);
                result.add(schemaTableTree1);
            }
        }
        return result;
    }

    public Pair<String, Set<SchemaTable>> constructJoinBetweenSchemaTablesAndStep(Set<SchemaTable> previousSchemaTables, VertexStep vertexStep) {
        String joinSql = "";
        Set<SchemaTable> schemaTables = new HashSet<>();
        for (SchemaTable previousSchemaTable : previousSchemaTables) {
            Pair<String, Set<SchemaTable>> joinSqlSchemaTables = constructJoinBetweenSchemaTableAndStep(previousSchemaTable, vertexStep);
            joinSql += joinSqlSchemaTables.getLeft();
            schemaTables.addAll(joinSqlSchemaTables.getRight());
        }
        return Pair.of(joinSql, schemaTables);
    }


    /**
     * Constructs a sql join statement between the given schemaTable and labels represented by the VertexStep.
     *
     * @param schemaTable The table to join on.
     * @param vertexStep  Join on to the tables the labels of the vertexStep links.
     * @return A Pair representing the sql join statement and a set of SchemaTables that were joined to.
     */
    public Pair<String, Set<SchemaTable>> constructJoinBetweenSchemaTableAndStep(SchemaTable schemaTable, VertexStep vertexStep) {
        String[] edgeLabels = vertexStep.getEdgeLabels();
        Direction direction = vertexStep.getDirection();
        Class elementClass = vertexStep.getReturnClass();

        Pair<Set<SchemaTable>, Set<SchemaTable>> labels = this.schemaManager.getLocalTableLabels().get(schemaTable);
        Set<SchemaTable> inLabels = labels.getLeft();
        Set<SchemaTable> outLabels = labels.getRight();
        Set<SchemaTable> inLabelsToTravers = new HashSet<>();
        Set<SchemaTable> outLabelsToTravers = new HashSet<>();
        switch (direction) {
            case IN:
                inLabelsToTravers = edgeLabels.length > 0 ? filter(inLabels, edgeLabels) : inLabels;
                break;
            case OUT:
                outLabelsToTravers = edgeLabels.length > 0 ? filter(outLabels, edgeLabels) : outLabels;
                break;
            case BOTH:
                inLabelsToTravers = edgeLabels.length > 0 ? filter(inLabels, edgeLabels) : inLabels;
                outLabelsToTravers = edgeLabels.length > 0 ? filter(outLabels, edgeLabels) : outLabels;
                break;
            default:
                throw new IllegalStateException("Unknown direction " + direction.name());
        }
        String joinSql = "";
        Set<SchemaTable> joinedSchemaTables = new HashSet<>();
        for (SchemaTable outLabelToTravers : outLabelsToTravers) {
            Pair<String, Set<SchemaTable>> joinSqlSchemaTables = constructJoinBetweenSchemaTables(schemaTable, Direction.OUT, outLabelToTravers);
            joinSql += joinSqlSchemaTables.getLeft();
            joinedSchemaTables.addAll(joinSqlSchemaTables.getRight());
        }
        for (SchemaTable inLabelToTravers : inLabelsToTravers) {
            Pair<String, Set<SchemaTable>> joinSqlSchemaTables = constructJoinBetweenSchemaTables(schemaTable, Direction.IN, inLabelToTravers);
            joinSql += joinSqlSchemaTables.getLeft();
            joinedSchemaTables.addAll(joinSqlSchemaTables.getRight());
        }
        return Pair.of(joinSql, joinedSchemaTables);
    }

    private Pair<String, Set<SchemaTable>> constructJoinBetweenSchemaTables(SchemaTable schemaTable, Direction direction, SchemaTable labelToTravers) {
        Set<SchemaTable> schemaTables = new HashSet<>();
        Map<String, Set<String>> edgeForeignKeys = this.schemaManager.getEdgeForeignKeys();
        String joinSql = " INNER JOIN ";
        joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema());
        joinSql += ".";
        joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable());
        joinSql += " ON ";
        joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema());
        joinSql += ".";
        joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getTable());
        joinSql += ".";
        joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID");
        joinSql += " = ";
        joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema());
        joinSql += ".";
        joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable());
        joinSql += ".";
        joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                schemaTable.getSchema() + "." + schemaTable.getTable().replace(SchemaManager.VERTEX_PREFIX, "") +
                        (direction == Direction.IN ? SchemaManager.IN_VERTEX_COLUMN_END : SchemaManager.OUT_VERTEX_COLUMN_END)
        );
        //join from the edge table to the incoming vertex table
        Set<String> foreignKeys = edgeForeignKeys.get(labelToTravers.toString());
        //Every foreignKey for the given direction must be joined on
        for (String foreignKey : foreignKeys) {
            String foreignKeySchema = foreignKey.split("\\.")[0];
            String foreignKeyTable = foreignKey.split("\\.")[1];
            if (direction == Direction.IN && foreignKey.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                joinSql += " INNER JOIN ";
                joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignKeySchema);
                joinSql += ".";
                joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + foreignKeyTable.replace(SchemaManager.OUT_VERTEX_COLUMN_END, ""));
                joinSql += " ON ";
                joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema());
                joinSql += ".";
                joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable());
                joinSql += ".";
                joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignKey);
                joinSql += " = ";
                joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignKeySchema);
                joinSql += ".";
                joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + foreignKeyTable.replace(SchemaManager.OUT_VERTEX_COLUMN_END, ""));
                joinSql += ".";
                joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
                schemaTables.add(SchemaTable.of(foreignKeySchema, SchemaManager.VERTEX_PREFIX + foreignKeyTable.replace(SchemaManager.OUT_VERTEX_COLUMN_END, "")));
            } else if (direction == Direction.OUT && foreignKey.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
                joinSql += " INNER JOIN ";
                joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignKeySchema);
                joinSql += ".";
                joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + foreignKeyTable.replace(SchemaManager.IN_VERTEX_COLUMN_END, ""));
                joinSql += " ON ";
                joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema());
                joinSql += ".";
                joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable());
                joinSql += ".";
                joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignKey);
                joinSql += " = ";
                joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignKeySchema);
                joinSql += ".";
                joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + foreignKeyTable.replace(SchemaManager.IN_VERTEX_COLUMN_END, ""));
                joinSql += ".";
                joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
                schemaTables.add(SchemaTable.of(foreignKeySchema, SchemaManager.VERTEX_PREFIX + foreignKeyTable.replace(SchemaManager.IN_VERTEX_COLUMN_END, "")));
            }
        }
        return Pair.of(joinSql, schemaTables);
    }

    private Set<SchemaTable> filter(Set<SchemaTable> labels, String[] edgeLabels) {
        Set<SchemaTable> result = new HashSet<>();
        List<String> edges = Arrays.asList(edgeLabels);
        for (SchemaTable label : labels) {
            String rawLabel = label.getTable().replace(SchemaManager.EDGE_PREFIX, "");
            if (edges.contains(rawLabel)) {
                result.add(label);
            }
        }
        return result;
    }

}
