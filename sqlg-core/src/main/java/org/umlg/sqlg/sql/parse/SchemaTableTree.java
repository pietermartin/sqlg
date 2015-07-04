package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.umlg.sqlg.structure.SchemaManager;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.Visitor;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Date: 2015/01/08
 * Time: 7:06 AM
 */
public class SchemaTableTree {
    //stepDepth indicates the depth of the replaced steps. i.e. v1.out().out().out() has stepDepth 0,1,2,3
    private int stepDepth;
    private SchemaTable schemaTable;
    private SchemaTableTree parent;
    //The root node does not have a direction. For the other nodes it indicates the direction from its parent to it.
    private Direction direction;
    private boolean isEdgeVertexStep;
    private List<SchemaTableTree> children = new ArrayList<>();
    private SqlgGraph sqlgGraph;
    //leafNodes is only set on the root node;
    private List<SchemaTableTree> leafNodes = new ArrayList<>();
    private List<HasContainer> hasContainers = new ArrayList<>();
    private Set<String> labels;

    SchemaTableTree(SqlgGraph sqlgGraph, SchemaTable schemaTable, int stepDepth) {
        this.sqlgGraph = sqlgGraph;
        this.schemaTable = schemaTable;
        this.stepDepth = stepDepth;
        this.hasContainers = new ArrayList<>();
        this.labels = new HashSet<>();
    }

    public SchemaTableTree addChild(
            SchemaTable schemaTable,
            Direction direction,
            Class<? extends Element> elementClass,
            List<HasContainer> hasContainers,
            int depth,
            boolean isEdgeVertexStep,
            Set<String> labels) {

        SchemaTableTree schemaTableTree = new SchemaTableTree(this.sqlgGraph, schemaTable, depth);
        if ((elementClass.isAssignableFrom(Edge.class) && schemaTable.getTable().startsWith(SchemaManager.EDGE_PREFIX)) ||
                (elementClass.isAssignableFrom(Vertex.class) && schemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX))) {
            schemaTableTree.hasContainers = new ArrayList<>(hasContainers);
        }
        schemaTableTree.parent = this;
        schemaTableTree.direction = direction;
        this.children.add(schemaTableTree);
        schemaTableTree.isEdgeVertexStep = isEdgeVertexStep;
        schemaTableTree.labels = labels;
        return schemaTableTree;
    }

    public SchemaTableTree addChild(SchemaTable schemaTable, Direction direction, Class<? extends Element> elementClass, List<HasContainer> hasContainers, int depth, Set<String> labels) {
        return addChild(schemaTable, direction, elementClass, hasContainers, depth, false, labels);
    }

    public SchemaTable getSchemaTable() {
        return schemaTable;
    }

    /**
     * @return A Triple. SchemaTableTree is the root of the tree that formed the sql statement.
     * It is needed to set the values in the where clause.
     * SchemaTable is the element being returned.
     * String is the sql.
     */
    public List<Pair<LinkedList<SchemaTableTree>, String>> constructSql() {
        Preconditions.checkState(this.parent == null, "constructSql may only be called on the root object");

        List<Pair<LinkedList<SchemaTableTree>, String>> result = new ArrayList<>();
        List<LinkedList<SchemaTableTree>> distinctQueries = constructDistinctQueries();
        for (LinkedList<SchemaTableTree> distinctQueryStack : distinctQueries) {

            //If the same element occurs multiple times in the stack then the sql needs to be different.
            //This is because the same element can not be joined on more than once in sql
            //The way to overcome this is  to break up the path in select sections with no duplicates and then join them together.
            if (duplicatesInStack(distinctQueryStack)) {
                List<LinkedList<SchemaTableTree>> subQueryStacks = splitIntoSubStacks(distinctQueryStack);
                String singlePathSql = constructDuplicatePathSql(this.sqlgGraph, subQueryStacks);
                result.add(Pair.of(distinctQueryStack, singlePathSql));
            } else {
                //If there are no duplicates in the path then one select statement will suffice.
                String singlePathSql = constructSinglePathSql(this.sqlgGraph, distinctQueryStack, null, null);
                result.add(Pair.of(distinctQueryStack, singlePathSql));
            }
        }
        return result;
    }

    private List<LinkedList<SchemaTableTree>> constructDistinctQueries() {
        Preconditions.checkState(this.parent == null, "constructSql may only be called on the root object");

        List<LinkedList<SchemaTableTree>> result = new ArrayList<>();
        for (SchemaTableTree leafNode : this.leafNodes) {
            result.add(leafNode.constructQueryStackFromLeaf());
        }
        return result;
    }

    /**
     * Construct a sql statement for one original path to a leaf node.
     * As the path contains the same label more than once it has been split into a List of Stacks.
     *
     * @param subQueryLinkedLists
     * @return
     */
    private static String constructDuplicatePathSql(SqlgGraph sqlgGraph, List<LinkedList<SchemaTableTree>> subQueryLinkedLists) {
        String singlePathSql = "";
        singlePathSql += "SELECT ";
        singlePathSql += constructOuterFromClause(sqlgGraph, subQueryLinkedLists.get(subQueryLinkedLists.size() - 1).getLast().getSchemaTable(), subQueryLinkedLists.size());
        singlePathSql += " FROM (";
        int count = 1;
        SchemaTableTree lastOfPrevious = null;
        for (LinkedList<SchemaTableTree> subQueryLinkedList : subQueryLinkedLists) {
            SchemaTableTree firstOfNext = null;
            boolean last = count == subQueryLinkedLists.size();
            if (!last) {
                //this is to get the next SchemaTable to join to
                LinkedList<SchemaTableTree> nextList = subQueryLinkedLists.get(count);
                firstOfNext = nextList.getFirst();
            }
            SchemaTableTree firstSchemaTableTree = subQueryLinkedList.getFirst();

            String sql = constructSinglePathSql(sqlgGraph, subQueryLinkedList, lastOfPrevious, firstOfNext);
            singlePathSql += sql;
            if (count == 1) {
                singlePathSql += ") a" + count++ + " INNER JOIN (";
            } else {
                //join the last with the first
                singlePathSql += ") a" + count + " ON ";
                singlePathSql += constructSectionedJoin(lastOfPrevious, firstSchemaTableTree, count);
                if (count++ < subQueryLinkedLists.size()) {
                    singlePathSql += " INNER JOIN (";
                }
            }
            lastOfPrevious = subQueryLinkedList.getLast();
        }
        return singlePathSql;
    }

    private static String constructOuterFromClause(SqlgGraph sqlgGraph, SchemaTable schemaTable, int count) {
        String sql = "a" + count + ".\"" + schemaTable.getSchema() + "." + schemaTable.getTable() + "." + SchemaManager.ID + "\"";
        Map<String, org.umlg.sqlg.structure.PropertyType> propertyTypeMap = sqlgGraph.getSchemaManager().getAllTables().get(schemaTable.toString());
        if (propertyTypeMap.size() > 0) {
            sql += ", ";
        }
        int propertyCount = 1;
        for (String propertyName : propertyTypeMap.keySet()) {
            sql += "a" + count + ".\"" + schemaTable.getSchema() + "." + schemaTable.getTable() + "." + propertyName + "\"";
            if (propertyCount++ < propertyTypeMap.size()) {
                sql += ",";
            }
        }
        return sql;
    }

    private static String constructSectionedJoin(SchemaTableTree fromSchemaTableTree, SchemaTableTree toSchemaTableTree, int count) {
        if (toSchemaTableTree.direction == Direction.BOTH) {
            throw new IllegalStateException("Direction may not be BOTH!");
        }
        String rawToLabel;
        if (toSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            rawToLabel = toSchemaTableTree.getSchemaTable().getTable().substring(SchemaManager.VERTEX_PREFIX.length());
        } else {
            rawToLabel = toSchemaTableTree.getSchemaTable().getTable();
        }
        String rawFromLabel;
        if (fromSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            rawFromLabel = fromSchemaTableTree.getSchemaTable().getTable().substring(SchemaManager.VERTEX_PREFIX.length());
        } else {
            rawFromLabel = fromSchemaTableTree.getSchemaTable().getTable();
        }

        String result;
        if (fromSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.EDGE_PREFIX)) {
            if (toSchemaTableTree.isEdgeVertexStep) {
                if (toSchemaTableTree.direction == Direction.OUT) {
                    result = "a" + (count - 1) + ".\"" + fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                            toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + SchemaManager.OUT_VERTEX_COLUMN_END + "\"";
                    result += " = a" + count + ".\"" + toSchemaTableTree.getSchemaTable().getSchema() + "." + toSchemaTableTree.getSchemaTable().getTable() + "." + SchemaManager.ID + "\"";
                } else {
                    result = "a" + (count - 1) + ".\"" + fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                            toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + SchemaManager.IN_VERTEX_COLUMN_END + "\"";
                    result += " = a" + count + ".\"" + toSchemaTableTree.getSchemaTable().getSchema() + "." + toSchemaTableTree.getSchemaTable().getTable() + "." + SchemaManager.ID + "\"";
                }
            } else {
                if (toSchemaTableTree.direction == Direction.OUT) {
                    result = "a" + (count - 1) + ".\"" + fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                            toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + SchemaManager.IN_VERTEX_COLUMN_END + "\"";
                    result += " = a" + count + ".\"" + toSchemaTableTree.getSchemaTable().getSchema() + "." + toSchemaTableTree.getSchemaTable().getTable() + "." + SchemaManager.ID + "\"";
                } else {
                    result = "a" + (count - 1) + ".\"" + fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                            toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + SchemaManager.OUT_VERTEX_COLUMN_END + "\"";
                    result += " = a" + count + ".\"" + toSchemaTableTree.getSchemaTable().getSchema() + "." + toSchemaTableTree.getSchemaTable().getTable() + "." + SchemaManager.ID + "\"";
                }
            }
        } else {
            if (toSchemaTableTree.direction == Direction.OUT) {
                result = "a" + (count - 1) + ".\"" + fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." + SchemaManager.ID + "\"";
                result += " = a" + count + ".\"" + toSchemaTableTree.getSchemaTable().getSchema() + "." + toSchemaTableTree.getSchemaTable().getTable() + "." +
                        fromSchemaTableTree.getSchemaTable().getSchema() + "." + rawFromLabel + SchemaManager.OUT_VERTEX_COLUMN_END + "\"";
            } else {
                result = "a" + (count - 1) + ".\"" + fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." + SchemaManager.ID + "\"";
                result += " = a" + count + ".\"" + toSchemaTableTree.getSchemaTable().getSchema() + "." + toSchemaTableTree.getSchemaTable().getTable() + "." +
                        fromSchemaTableTree.getSchemaTable().getSchema() + "." + rawFromLabel + SchemaManager.IN_VERTEX_COLUMN_END + "\"";
            }
        }
        return result;
    }

    /**
     * Constructs a sql select statement from the SchemaTableTree call stack.
     * The SchemaTableTree is not used as a tree. It is used only as as SchemaTable with a direction.
     * first and last is needed to facilitate generating the from statement.
     * If both first and last is true then the gremlin does not contain duplicate labels in its path and
     * can be executed in one sql statement.
     * If first and last is not equal then the sql will join across many select statements.
     * The previous select needs to join onto the subsequent select. For this the from needs to select the appropriate
     * field for the join.
     *
     * @param distinctQueryStack
     * @param lastOfPrevious
     * @return
     */
    private static String constructSinglePathSql(SqlgGraph sqlgGraph, LinkedList<SchemaTableTree> distinctQueryStack, SchemaTableTree lastOfPrevious, SchemaTableTree firstOfNextStack) {
        String singlePathSql = "SELECT ";
        SchemaTableTree firstSchemaTableTree = distinctQueryStack.getFirst();
        SchemaTableTree lastSchemaTableTree = distinctQueryStack.getLast();
        SchemaTable firstSchemaTable = firstSchemaTableTree.getSchemaTable();
        SchemaTable lastSchemaTable = lastSchemaTableTree.getSchemaTable();
        singlePathSql += constructFromClause(sqlgGraph, firstSchemaTableTree, lastSchemaTableTree, lastOfPrevious, firstOfNextStack);
        singlePathSql += " FROM ";
        singlePathSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTableTree.getSchemaTable().getSchema());
        singlePathSql += ".";
        singlePathSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTableTree.getSchemaTable().getTable());
        SchemaTableTree previous = firstSchemaTableTree;
        boolean skipFirst = true;
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            if (skipFirst) {
                skipFirst = false;
                continue;
            }
            singlePathSql += constructJoinBetweenSchemaTables(sqlgGraph, previous, schemaTableTree);
            previous = schemaTableTree;
        }

        //lastOfPrevious is null for the first call in the call stack it needs the id parameter in the where clause.
        if (lastOfPrevious == null) {
            singlePathSql += " WHERE ";
            singlePathSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema());
            singlePathSql += ".";
            singlePathSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable());
            singlePathSql += "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
            singlePathSql += " = ? ";
        }

        //check if the 'where' has already been printed
        boolean printedWhere = lastOfPrevious == null;

        //construct there where clause for the hasContainers
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            for (HasContainer hasContainer : schemaTableTree.getHasContainers()) {
                if (!printedWhere) {
                    printedWhere = true;
                    singlePathSql += " WHERE ";
                } else {
                    singlePathSql += " AND ";
                }
                singlePathSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTableTree.getSchemaTable().getSchema());
                singlePathSql += ".";
                singlePathSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTableTree.getSchemaTable().getTable());
                if (hasContainer.getKey().equals(T.id.getAccessor())) {
                    singlePathSql += ".\"ID\"";
                } else {
                    singlePathSql += "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
                }
                singlePathSql += " = ?";
            }
        }

        return singlePathSql;
    }

    private static List<LinkedList<SchemaTableTree>> splitIntoSubStacks(LinkedList<SchemaTableTree> distinctQueryStack) {
        List<LinkedList<SchemaTableTree>> result = new ArrayList<>();
        LinkedList<SchemaTableTree> subList = new LinkedList<>();
        result.add(subList);
        Set<SchemaTable> alreadyVisited = new HashSet<>();
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            if (!alreadyVisited.contains(schemaTableTree.getSchemaTable())) {
                alreadyVisited.add(schemaTableTree.getSchemaTable());
                subList.add(schemaTableTree);
            } else {
                alreadyVisited.clear();
                subList = new LinkedList<>();
                subList.add(schemaTableTree);
                result.add(subList);
                alreadyVisited.add(schemaTableTree.getSchemaTable());
            }
        }
        return result;
    }

    /**
     * Checks if the stack has the same element more than once.
     *
     * @param distinctQueryStack
     * @return true is there are duplicates else false
     */
    private static boolean duplicatesInStack(LinkedList<SchemaTableTree> distinctQueryStack) {
        Set<SchemaTable> alreadyVisited = new HashSet<>();
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            if (!alreadyVisited.contains(schemaTableTree.getSchemaTable())) {
                alreadyVisited.add(schemaTableTree.getSchemaTable());
            } else {
                return true;
            }
        }
        return false;
    }

    /**
     * Constructs the from clause with the required selected fields needed to make the join between the previous and the next SchemaTable
     *
     * @param firstSchemaTableTree    This is the first SchemaTable in the current sql stack. If it is an Edge table then its foreign key
     *                                field to the previous table need to be in the select clause in order for the join statement to
     *                                reference it.
     * @param lastSchemaTableTree
     * @param previousSchemaTableTree The previous schemaTableTree that will be joined to.
     * @param nextSchemaTableTree     represents the table to join to. it is null for the last table as there is nothing to join to.  @return
     */
    private static String constructFromClause(
            SqlgGraph sqlgGraph,
            SchemaTableTree firstSchemaTableTree,
            SchemaTableTree lastSchemaTableTree,
            SchemaTableTree previousSchemaTableTree,
            SchemaTableTree nextSchemaTableTree) {

        SchemaTable firstSchemaTable = firstSchemaTableTree.getSchemaTable();
        SchemaTable lastSchemaTable = lastSchemaTableTree.getSchemaTable();

        if (previousSchemaTableTree != null && previousSchemaTableTree.direction == Direction.BOTH) {
            throw new IllegalStateException("Direction should never be BOTH");
        }
        if (nextSchemaTableTree != null && nextSchemaTableTree.direction == Direction.BOTH) {
            throw new IllegalStateException("Direction should never be BOTH");
        }
        //The join is always between an edge and vertex or vertex and edge table.
        if (nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)
                && nextSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            throw new IllegalStateException("Join can not be between 2 vertex tables!");
        }
        if (nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(SchemaManager.EDGE_PREFIX)
                && nextSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.EDGE_PREFIX)) {
            throw new IllegalStateException("Join can not be between 2 edge tables!");
        }

        if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)
                && previousSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            throw new IllegalStateException("Join can not be between 2 vertex tables!");
        }
        if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(SchemaManager.EDGE_PREFIX)
                && previousSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.EDGE_PREFIX)) {
            throw new IllegalStateException("Join can not be between 2 edge tables!");
        }

        String sql = "";
        boolean printedId = false;

        //join to the previous label/table
        if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(SchemaManager.EDGE_PREFIX)) {
            if (!previousSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
                throw new IllegalStateException("Expected table to start with " + SchemaManager.VERTEX_PREFIX);
            }
            String rawLabel = previousSchemaTableTree.getSchemaTable().getTable().substring(SchemaManager.VERTEX_PREFIX.length());
            if (firstSchemaTableTree.direction == Direction.OUT) {
                sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()) + "." +
                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()) + "." +
                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                previousSchemaTableTree.getSchemaTable().getSchema() + "." +
                                        rawLabel + SchemaManager.OUT_VERTEX_COLUMN_END
                        );
                sql += " AS \"" + firstSchemaTable.getSchema() + "." + firstSchemaTable.getTable() + "." +
                        previousSchemaTableTree.getSchemaTable().getSchema() + "." +
                        rawLabel + SchemaManager.OUT_VERTEX_COLUMN_END + "\"";
            } else {
                sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()) + "." +
                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()) + "." +
                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                previousSchemaTableTree.getSchemaTable().getSchema() + "." +
                                        rawLabel + SchemaManager.IN_VERTEX_COLUMN_END
                        );
                sql += " AS \"" + firstSchemaTable.getSchema() + "." + firstSchemaTable.getTable() + "." +
                        previousSchemaTableTree.getSchemaTable().getSchema() + "." +
                        rawLabel + SchemaManager.IN_VERTEX_COLUMN_END + "\"";
            }
        } else if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()) + "." +
                    sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()) + "." +
                    sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
            sql += " AS \"" + firstSchemaTable.getSchema() + "." + firstSchemaTable.getTable() + "." + SchemaManager.ID + "\"";
            printedId = firstSchemaTable == lastSchemaTable;
        }

        //join to the next table/label
        if (nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(SchemaManager.EDGE_PREFIX)) {
            if (!nextSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
                throw new IllegalStateException("Expected table to start with " + SchemaManager.VERTEX_PREFIX);
            }
            String rawLabel = nextSchemaTableTree.getSchemaTable().getTable().substring(SchemaManager.VERTEX_PREFIX.length());
            if (!sql.isEmpty()) {
                sql += ", ";
            }
            if (nextSchemaTableTree.direction == Direction.OUT) {
                if (nextSchemaTableTree.isEdgeVertexStep) {
                    sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getSchema()) + "." +
                            sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getTable()) + "." +
                            sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                    nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                            rawLabel + SchemaManager.OUT_VERTEX_COLUMN_END
                            );
                    sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                            nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                            rawLabel + SchemaManager.OUT_VERTEX_COLUMN_END + "\"";
                } else {
                    sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getSchema()) + "." +
                            sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getTable()) + "." +
                            sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                    nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                            rawLabel + SchemaManager.IN_VERTEX_COLUMN_END
                            );
                    sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                            nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                            rawLabel + SchemaManager.IN_VERTEX_COLUMN_END + "\"";
                }
            } else {
                if (nextSchemaTableTree.isEdgeVertexStep) {
                    sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getSchema()) + "." +
                            sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getTable()) + "." +
                            sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                    nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                            rawLabel + SchemaManager.IN_VERTEX_COLUMN_END
                            );
                    sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                            nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                            rawLabel + SchemaManager.IN_VERTEX_COLUMN_END + "\"";
                } else {
                    sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getSchema()) + "." +
                            sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getTable()) + "." +
                            sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                    nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                            rawLabel + SchemaManager.OUT_VERTEX_COLUMN_END
                            );
                    sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                            nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                            rawLabel + SchemaManager.OUT_VERTEX_COLUMN_END + "\"";
                }
            }
        } else if (nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            if (!sql.isEmpty()) {
                sql += ", ";
            }
            sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getSchema()) + "." +
                    sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getTable()) + "." +
                    sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
            sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." + SchemaManager.ID + "\"";
            printedId = firstSchemaTable == lastSchemaTable;
        }

        //The last schemaTableTree in the call stack as no nextSchemaTableTree.
        //This last element's properties need to be returned.
        if (nextSchemaTableTree == null) {
            sql = printIDFromClauseFor(sqlgGraph, lastSchemaTableTree, sql);
            sql += ", ";
            sql = printFromClauseFor(sqlgGraph, lastSchemaTableTree, sql, printedId);
            //all labeled step's properties also need to be returned
            List<SchemaTableTree> labeledSteps = firstSchemaTableTree.getLabeledSteps();
            if (!labeledSteps.isEmpty()) {
                sql += ", ";
            }
            int count = 1;
            for (SchemaTableTree labelledStep : labeledSteps) {
                sql = printLabeledIDFromClauseFor(sqlgGraph, labelledStep, sql);
                Map<String, org.umlg.sqlg.structure.PropertyType> propertyTypeMap = sqlgGraph.getSchemaManager().getAllTables().get(labelledStep.getSchemaTable().toString());
                if (!propertyTypeMap.isEmpty()) {
                    sql += ", ";
                }
                sql = printLabeledFromClauseFor(sqlgGraph, labelledStep, sql, false);
                if (!labelledStep.getSchemaTable().isVertexTable()) {
                    sql += ", ";
                    sql = printLabeledEdgeInOutVertexIdFromClauseFor(sqlgGraph, labelledStep, sql, false);
                }
                if (count++ < labeledSteps.size()) {
                    sql += ", ";
                }
            }

        }
        return sql;
    }

    private static String printIDFromClauseFor(SqlgGraph sqlgGraph, SchemaTableTree lastSchemaTableTree, String sql) {
        String finalFromSchemaTableName = sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getSchema());
        finalFromSchemaTableName += ".";
        finalFromSchemaTableName += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getTable());
        if (!sql.isEmpty()) {
            sql += ", ";
        }
        sql += finalFromSchemaTableName + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
        sql += " AS \"";
        sql += lastSchemaTableTree.aliasId();
        sql += "\"";
        return sql;
    }

    private static String printFromClauseFor(SqlgGraph sqlgGraph, SchemaTableTree lastSchemaTableTree, String sql, boolean printedId) {
        Map<String, org.umlg.sqlg.structure.PropertyType> propertyTypeMap = sqlgGraph.getSchemaManager().getAllTables().get(lastSchemaTableTree.getSchemaTable().toString());
        String finalFromSchemaTableName = sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getSchema());
        finalFromSchemaTableName += ".";
        finalFromSchemaTableName += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getTable());
        int propertyCount = 1;
        for (String propertyName : propertyTypeMap.keySet()) {
            sql += finalFromSchemaTableName + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(propertyName);
            sql += " AS \"";
            sql += lastSchemaTableTree.aliasPropertyName(propertyName);
            sql += "\"";
            if (propertyCount++ < propertyTypeMap.size()) {
                sql += ",";
            }
        }
        return sql;
    }

    private static String printLabeledIDFromClauseFor(SqlgGraph sqlgGraph, SchemaTableTree lastSchemaTableTree, String sql) {
        String finalFromSchemaTableName = sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getSchema());
        finalFromSchemaTableName += ".";
        finalFromSchemaTableName += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getTable());
        sql += finalFromSchemaTableName + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
        sql += " AS \"";
        sql += lastSchemaTableTree.labeledAliasId();
        sql += "\"";
        return sql;
    }

    private static String printLabeledFromClauseFor(SqlgGraph sqlgGraph, SchemaTableTree lastSchemaTableTree, String sql, boolean printedId) {
        Map<String, org.umlg.sqlg.structure.PropertyType> propertyTypeMap = sqlgGraph.getSchemaManager().getAllTables().get(lastSchemaTableTree.getSchemaTable().toString());
        String finalFromSchemaTableName = sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getSchema());
        finalFromSchemaTableName += ".";
        finalFromSchemaTableName += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getTable());
        int propertyCount = 1;
        for (String propertyName : propertyTypeMap.keySet()) {
            sql += finalFromSchemaTableName + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(propertyName);
            sql += " AS \"";
            sql += lastSchemaTableTree.labeledAliasPropertyName(propertyName);
            sql += "\"";
        }
        return sql;
    }

    private static String printLabeledEdgeInOutVertexIdFromClauseFor(SqlgGraph sqlgGraph, SchemaTableTree lastSchemaTableTree, String sql, boolean printedId) {
        Preconditions.checkState(!lastSchemaTableTree.getSchemaTable().isVertexTable());

        String finalFromSchemaTableName = sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getSchema());
        finalFromSchemaTableName += ".";
        finalFromSchemaTableName += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getTable());

        Set<String> edgeForeignKeys = sqlgGraph.getSchemaManager().getAllEdgeForeignKeys().get(lastSchemaTableTree.getSchemaTable().toString());
        int propertyCount = 1;
        for (String edgeForeignKey : edgeForeignKeys) {
            sql += finalFromSchemaTableName + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeForeignKey);
            sql += " AS \"";
            sql += lastSchemaTableTree.labeledAliasPropertyName(edgeForeignKey);
            sql += "\"";
            if (propertyCount++ < edgeForeignKeys.size()) {
                sql += ", ";
            }
        }
        return sql;

//        Map<String, org.umlg.sqlg.structure.PropertyType> propertyTypeMap = sqlgGraph.getSchemaManager().getAllTables().get(lastSchemaTableTree.getSchemaTable().toString());
//        String finalFromSchemaTableName = sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getSchema());
//        finalFromSchemaTableName += ".";
//        finalFromSchemaTableName += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getTable());
//        int propertyCount = 1;
//        for (String propertyName : propertyTypeMap.keySet()) {
//            sql += finalFromSchemaTableName + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(propertyName);
//            sql += " AS \"";
//            sql += lastSchemaTableTree.labeledAliasPropertyName(propertyName);
//            sql += "\"";
//            if (propertyCount++ < propertyTypeMap.size()) {
//                sql += ",";
//            }
//        }
//        return sql;
    }

    public String aliasId() {
        return getSchemaTable().getSchema() + "." + getSchemaTable().getTable() + "." + SchemaManager.ID;
    }

    public String aliasPropertyName(String propertyName) {
        return getSchemaTable().getSchema() + "." + getSchemaTable().getTable() + "." + propertyName;
    }

    public String propertyNameFromAlias(String alias) {
        return alias.replace(getSchemaTable().getSchema() + "." + getSchemaTable().getTable() + ".", "");
    }

    public String labeledAliasId() {
        String labels = reducedLabels();
        return labels + "." + getSchemaTable().getSchema() + "." + getSchemaTable().getTable() + "." + SchemaManager.ID;
    }

    public String labeledAliasPropertyName(String propertyName) {
        String labels = reducedLabels();
        return labels + "." + getSchemaTable().getSchema() + "." + getSchemaTable().getTable() + "." + propertyName;
    }

    public String propertyNameFromLabeledAlias(String alias) {
        String labels = reducedLabels();
        return alias.replace(labels + "." + getSchemaTable().getSchema() + "." + getSchemaTable().getTable() + ".", "");
    }

    public String reducedLabels() {
        String labels = getLabels().stream().reduce((a, b) -> a + "." + b).get();
        return labels;
    }

    private LinkedList<SchemaTableTree> constructQueryStackFromLeaf() {
        LinkedList<SchemaTableTree> queryCallStack = new LinkedList<>();
        SchemaTableTree node = this;
        while (node != null) {
            queryCallStack.add(0, node);
            node = node.parent;
        }
        return queryCallStack;
    }

    private static String constructJoinBetweenSchemaTables(SqlgGraph sqlgGraph, SchemaTableTree fromSchemaTableTree, SchemaTableTree labelToTraversTree) {
        SchemaTable fromSchemaTable = fromSchemaTableTree.getSchemaTable();
        SchemaTable labelToTravers = labelToTraversTree.getSchemaTable();

        //Assert that this is always from vertex to edge table or edge to vertex table
        Preconditions.checkState(
                (fromSchemaTable.isVertexTable() && !labelToTravers.isVertexTable()) ||
                        (!fromSchemaTable.isVertexTable() && labelToTravers.isVertexTable())
        );

        String rawLabel;
        if (fromSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            rawLabel = fromSchemaTable.getTable().substring(SchemaManager.VERTEX_PREFIX.length());
        } else {
            rawLabel = fromSchemaTable.getTable();
        }
        String rawLabelToTravers;
        if (labelToTravers.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            rawLabelToTravers = labelToTravers.getTable().substring(SchemaManager.VERTEX_PREFIX.length());
        } else {
            rawLabelToTravers = labelToTravers.getTable();
        }
        String joinSql = " INNER JOIN ";
        if (fromSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable());
            joinSql += " ON ";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getSchema());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getTable());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID");
            joinSql += " = ";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                    fromSchemaTable.getSchema() + "." + rawLabel +
                            (labelToTraversTree.getDirection() == Direction.IN ? SchemaManager.IN_VERTEX_COLUMN_END : SchemaManager.OUT_VERTEX_COLUMN_END)
            );
        } else {
            //From edge to vertex table the foreign key is opposite to the direction.
            //This is because this is second part of the traversal via the edge.
            //This code did not take specific traversals from the edge into account.

            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable());
            joinSql += " ON ";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getSchema());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getTable());
            joinSql += ".";
            if (labelToTraversTree.isEdgeVertexStep) {
                joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema() + "." +
                        rawLabelToTravers + (labelToTraversTree.getDirection() == Direction.OUT ? SchemaManager.OUT_VERTEX_COLUMN_END : SchemaManager.IN_VERTEX_COLUMN_END));
            } else {
                joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema() + "." +
                        rawLabelToTravers + (labelToTraversTree.getDirection() == Direction.OUT ? SchemaManager.IN_VERTEX_COLUMN_END : SchemaManager.OUT_VERTEX_COLUMN_END));
            }
            joinSql += " = ";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID");
        }
        return joinSql;
    }

    /**
     * Remove all leaf nodes that are not at the deepest level.
     * Those nodes are not to be included in the sql as they do not have enough incident edges.
     * i.e. The graph is not deep enough along those labels.
     * <p>
     * This is done via a breath first traversal.
     */
    void removeAllButDeepestLeafNodes(int depth) {
        Queue<SchemaTableTree> queue = new LinkedList<>();
        queue.add(this);
        while (!queue.isEmpty()) {
            SchemaTableTree current = queue.remove();
            if (current.stepDepth < depth && current.children.isEmpty()) {
                removeNode(current);
            } else {
                queue.addAll(current.children);
                if (current.stepDepth == depth && current.children.isEmpty()) {
                    this.leafNodes.add(current);
                }
            }
        }
    }

    private void removeNode(SchemaTableTree node) {
        SchemaTableTree parent = node.parent;
        if (parent != null) {
            parent.children.remove(node);
            this.leafNodes.remove(node);
            //check if the parent has any other children. if not it too can be deleted. Follow this pattern recursively up.
            if (parent.children.isEmpty()) {
                removeNode(parent);
            }
        }
    }

    void removeNodesInvalidatedByHas() {
        Queue<SchemaTableTree> queue = new LinkedList<>();
        queue.add(this);
        while (!queue.isEmpty()) {
            SchemaTableTree current = queue.remove();
            if (invalidateByHas(current)) {
                removeNode(current);
            } else {
                queue.addAll(current.children);
            }
        }
    }

    private boolean invalidateByHas(SchemaTableTree schemaTableTree) {
        Set<HasContainer> toRemove = new HashSet<>();
        for (HasContainer hasContainer : schemaTableTree.hasContainers) {
            //Check if we are on a vertex or edge
            SchemaTable hasContainerLabelSchemaTable;
            if (schemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
                hasContainerLabelSchemaTable = SchemaTable.from(this.sqlgGraph, SchemaManager.VERTEX_PREFIX + hasContainer.getValue().toString(), this.sqlgGraph.getSqlDialect().getPublicSchema());
            } else {
                hasContainerLabelSchemaTable = SchemaTable.from(this.sqlgGraph, SchemaManager.EDGE_PREFIX + hasContainer.getValue().toString(), this.sqlgGraph.getSqlDialect().getPublicSchema());
            }
            if (hasContainer.getKey().equals(T.label.getAccessor()) && hasContainer.getBiPredicate().equals(Compare.eq) &&
                    !hasContainerLabelSchemaTable.toString().equals(schemaTableTree.getSchemaTable().toString())) {
                return true;
            } else if (hasContainer.getKey().equals(T.label.getAccessor()) && hasContainer.getBiPredicate().equals(Compare.eq) &&
                    hasContainerLabelSchemaTable.toString().equals(schemaTableTree.getSchemaTable().toString())) {

                //remove the hasContainer as the query is already fulfilling it.
                toRemove.add(hasContainer);
            }
        }
        schemaTableTree.hasContainers.removeAll(toRemove);
        return false;
    }

    @Override
    public String toString() {
        return this.schemaTable.toString();
    }

    public String toTreeString() {
        StringBuilder result = new StringBuilder();
        internalToString(result);
        return result.toString();
    }

    private void internalToString(StringBuilder sb) {
        if (sb.length() > 0) {
            sb.append("\n");
        }
        for (int i = 0; i < this.stepDepth; i++) {
            sb.append("\t");
        }
        sb.append(this.schemaTable.toString()).append(" ")
                .append(this.stepDepth).append(" ")
                .append(this.hasContainers.toString()).append(" ")
                .append(this.direction != null ? this.direction.toString() : "").append(" ")
                .append("isVertexStep = ").append(this.isEdgeVertexStep)
                .append(" labels = ").append(this.labels);
        for (SchemaTableTree child : children) {
            child.internalToString(sb);
        }
    }

    public SchemaTableTree getParent() {
        return parent;
    }

    public Direction getDirection() {
        return direction;
    }

    public List<HasContainer> getHasContainers() {
        return hasContainers;
    }

    public int depth() {
        AtomicInteger depth = new AtomicInteger();
        walk(v -> {
            if (v.stepDepth > depth.get()) {
                depth.set(v.stepDepth);
            }
            return null;
        });
        return depth.incrementAndGet();
    }

    public int numberOfNodes() {
        AtomicInteger count = new AtomicInteger();
        walk(v -> {
            count.getAndIncrement();
            return null;
        });
        return count.get();
    }

    private void walk(Visitor v) {
        v.visit(this);
        this.children.forEach(c -> c.walk(v));
    }

    public SchemaTableTree schemaTableAtDepth(final int depth, final int number) {
        AtomicInteger count = new AtomicInteger();
        //Need to reset the count when the depth changes.
        AtomicInteger depthCache = new AtomicInteger(depth);
        return walkWithExit(
                v -> {
                    if (depthCache.get() != v.stepDepth) {
                        depthCache.set(v.stepDepth);
                        count.set(0);
                    }
                    return (count.getAndIncrement() == number && v.stepDepth == depth);
                }
        );
    }

    private SchemaTableTree walkWithExit(Visitor<Boolean> v) {
        if (!v.visit(this)) {
            for (SchemaTableTree child : children) {
                return child.walkWithExit(v);
            }
        }
        return this;
    }

    @Override
    public int hashCode() {
        if (this.parent != null) {
            if (this.direction == null) {
                return (this.schemaTable.toString() + this.parent.toString()).hashCode();
            } else {
                return (this.schemaTable.toString() + this.direction.name() + this.parent.toString()).hashCode();
            }
        } else {
            if (this.direction == null) {
                return this.schemaTable.toString().hashCode();
            } else {
                return (this.schemaTable.toString() + this.direction.name()).hashCode();
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof SchemaTableTree)) {
            return false;
        }
        if (o == this) {
            return true;
        }
        SchemaTableTree other = (SchemaTableTree) o;
        if (this.direction != other.direction) {
            return false;
        } else if (this.parent != null && other.parent == null) {
            return false;
        } else if (this.parent == null && other.parent != null) {
            return false;
        } else if (this.parent == null && other.parent == null) {
            return this.schemaTable.equals(other.parent);
        } else {
            return this.parent.equals(other.parent) && this.schemaTable.equals(other.schemaTable);
        }
    }

    public List<SchemaTableTree> getLabeledSteps() {
        List<SchemaTableTree> result = new ArrayList<>();
        walk(v -> {
            if (!v.labels.isEmpty()) {
                result.add(v);
            }
            return null;
        });
        return result;
    }

    public Set<String> getLabels() {
        return labels;
    }
}
