package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.graph.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * Date: 2015/01/03
 * Time: 1:06 PM
 */
public class GremlinParser<E extends Element> {

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
     * @param schemaTable
     * @param elements
     * @throws SQLException
     */
    public void loadElements(ResultSet resultSet, SchemaTable schemaTable, List<Element> elements) throws SQLException {
        String idProperty = schemaTable.getSchema() + "." + schemaTable.getTable() + "." + SchemaManager.ID;
        Long id = resultSet.getLong(idProperty);
        SqlgElement sqlgElement;
        if (schemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            String rawLabel = schemaTable.getTable().substring(SchemaManager.VERTEX_PREFIX.length());
            sqlgElement = SqlgVertex.of(this.sqlgGraph, id, schemaTable.getSchema(), rawLabel);
        } else {
            if (!schemaTable.getTable().startsWith(SchemaManager.EDGE_PREFIX)) {
                throw new IllegalStateException("Expected table to start with " + SchemaManager.EDGE_PREFIX);
            }
            String rawLabel = schemaTable.getTable().substring(SchemaManager.EDGE_PREFIX.length());
            sqlgElement = new SqlgEdge(this.sqlgGraph, id, schemaTable.getSchema(), rawLabel);
        }
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnName(i);
            Object o = resultSet.getObject(columnName);
            if (!columnName.equals("ID")
                    && !columnName.equals(SchemaManager.VERTEX_IN_LABELS)
                    && !columnName.equals(SchemaManager.VERTEX_OUT_LABELS)
                    && !columnName.equals(SchemaManager.VERTEX_SCHEMA)
                    && !columnName.equals(SchemaManager.VERTEX_TABLE)
                    && !Objects.isNull(o)) {

                sqlgElement.loadProperty(resultSetMetaData, i, columnName, o);
            }
        }
        sqlgElement.properties.clear();
        elements.add(sqlgElement);
    }

    /**
     * Constructs the label paths from the given schemaTable to the leaf vertex labels for the gremlin query.
     * For each path Sqlg will execute a sql query. The union of the queries is the result the gremlin query.
     * The vertex labels can be calculated from the steps.
     *
     * @param schemaTable
     * @param replacedSteps The original VertexSteps and HasSteps that were replaced
     * @return a List of paths. Each path is itself a list of SchemaTables.
     */
    public SchemaTableTree parse(SchemaTable schemaTable, List<Pair<VertexStep<E>, List<HasContainer>>> replacedSteps) {
        Set<SchemaTableTree> schemaTableTrees = new HashSet<>();
        SchemaTableTree rootSchemaTableTree = new SchemaTableTree(this.sqlgGraph, schemaTable, 0);
        schemaTableTrees.add(rootSchemaTableTree);
        int depth = 1;
        for (Pair<VertexStep<E>, List<HasContainer>> replacedStep : replacedSteps) {
            schemaTableTrees = calculatePathForVertexStep(schemaTableTrees, replacedStep, depth++);
        }
        rootSchemaTableTree.removeAllButDeepestLeafNodes(replacedSteps.size());
        rootSchemaTableTree.removeNodesInvalidatedByHas();
        return rootSchemaTableTree;
    }

    private Set<SchemaTableTree> calculatePathForVertexStep(Set<SchemaTableTree> schemaTableTrees, Pair<VertexStep<E>, List<HasContainer>> replacedStep, int depth) {
        Set<SchemaTableTree> result = new HashSet<>();
        for (SchemaTableTree schemaTableTree : schemaTableTrees) {
            String[] edgeLabels = replacedStep.getLeft().getEdgeLabels();
            Direction direction = replacedStep.getLeft().getDirection();
            Class<? extends Element> elementClass = replacedStep.getLeft().getReturnClass();
            List<HasContainer> hasContainers = replacedStep.getRight();

            Pair<Set<SchemaTable>, Set<SchemaTable>> labels = this.schemaManager.getTableLabels(schemaTableTree.getSchemaTable());
            Set<SchemaTable> inLabels = labels != null ? labels.getLeft() : new HashSet<>();
            Set<SchemaTable> outLabels = labels != null ? labels.getRight() : new HashSet<>();
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
                SchemaTableTree schemaTableTreeChild = schemaTableTree.addChild(inLabelsToTravers, Direction.IN, elementClass, hasContainers, depth);
                if (elementClass.isAssignableFrom(Edge.class)) {
                    result.add(schemaTableTreeChild);
                } else {
                    result.addAll(calculatePathFromEdgeToVertex(schemaTableTreeChild, inLabelsToTravers, Direction.IN, elementClass, hasContainers, depth));
                }
            }
            for (SchemaTable outLabelsToTravers : outLabelsToTraversers) {
                SchemaTableTree schemaTableTreeChild = schemaTableTree.addChild(outLabelsToTravers, Direction.OUT, elementClass, hasContainers, depth);
                if (elementClass.isAssignableFrom(Edge.class)) {
                    result.add(schemaTableTreeChild);
                } else {
                    result.addAll(calculatePathFromEdgeToVertex(schemaTableTreeChild, outLabelsToTravers, Direction.OUT, elementClass, hasContainers, depth));
                }
            }
        }
        return result;
    }

    private Set<SchemaTableTree> calculatePathFromEdgeToVertex(SchemaTableTree schemaTableTree, SchemaTable labelToTravers, Direction direction, Class<? extends Element> elementClass, List<HasContainer> hasContainers, int depth) {
        Set<SchemaTableTree> result = new HashSet<>();
        Map<String, Set<String>> edgeForeignKeys = this.schemaManager.getAllEdgeForeignKeys();
        //join from the edge table to the incoming vertex table
        Set<String> foreignKeys = edgeForeignKeys.get(labelToTravers.toString());
        //Every foreignKey for the given direction must be joined on
        for (String foreignKey : foreignKeys) {
            String foreignKeySchema = foreignKey.split("\\.")[0];
            String foreignKeyTable = foreignKey.split("\\.")[1];
            if (direction == Direction.IN && foreignKey.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                SchemaTableTree schemaTableTree1 = schemaTableTree.addChild(
                        SchemaTable.of(foreignKeySchema, SchemaManager.VERTEX_PREFIX + foreignKeyTable.replace(SchemaManager.OUT_VERTEX_COLUMN_END, "")),
                        direction,
                        elementClass,
                        hasContainers,
                        depth);
                result.add(schemaTableTree1);
            } else if (direction == Direction.OUT && foreignKey.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
                SchemaTableTree schemaTableTree1 = schemaTableTree.addChild(
                        SchemaTable.of(foreignKeySchema, SchemaManager.VERTEX_PREFIX + foreignKeyTable.replace(SchemaManager.IN_VERTEX_COLUMN_END, "")),
                        direction,
                        elementClass,
                        hasContainers,
                        depth);
                result.add(schemaTableTree1);
            }
        }
        return result;
    }

    private Set<SchemaTable> filter(Set<SchemaTable> labels, String[] edgeLabels) {
        Set<SchemaTable> result = new HashSet<>();
        List<String> edges = Arrays.asList(edgeLabels);
        for (SchemaTable label : labels) {
            if (!label.getTable().startsWith(SchemaManager.EDGE_PREFIX)) {
                throw new IllegalStateException("Expected label to start with " + SchemaManager.EDGE_PREFIX);
            }
            String rawLabel = label.getTable().substring(SchemaManager.EDGE_PREFIX.length());
            if (edges.contains(rawLabel)) {
                result.add(label);
            }
        }
        return result;
    }

}
