package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.structure.Direction;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

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
    private List<SchemaTableTree> children = new ArrayList<>();
    private SqlgGraph sqlgGraph;
    //leafNodes is only set on the root node;
    private List<SchemaTableTree> leafNodes = new ArrayList<>();

    SchemaTableTree(SqlgGraph sqlgGraph, SchemaTable schemaTable, int stepDepth) {
        this.sqlgGraph = sqlgGraph;
        this.schemaTable = schemaTable;
        this.stepDepth = stepDepth;
    }

    SchemaTableTree addChild(SchemaTable schemaTable, Direction direction, int depth) {
        SchemaTableTree schemaTableTree = new SchemaTableTree(this.sqlgGraph, schemaTable, depth);
        schemaTableTree.parent = this;
        schemaTableTree.direction = direction;
        this.children.add(schemaTableTree);
        return schemaTableTree;
    }

    SchemaTable getSchemaTable() {
        return schemaTable;
    }

    /**
     * returns A separate sql statement for every leaf node.
     *
     * @return a List of sql statements
     */
    List<Pair<SchemaTable, String>> constructSql() {
        List<Pair<SchemaTable, String>> result = new ArrayList<>();
        List<LinkedList<SchemaTableTree>> distinctQueries = new ArrayList<>();
        for (SchemaTableTree leafNode : this.leafNodes) {
            distinctQueries.add(constructQueryStackFromLeaf(leafNode));
        }
        for (LinkedList<SchemaTableTree> distinctQueryStack : distinctQueries) {

            //If the same element occurs multiple times in the stack then the sql needs to be different.
            //This is because the same element can not be joined on more than once in sql
            //The way to overcome this is  to break up the path in select sections with no duplicates and then join them together.
            if (duplicatesInStack(distinctQueryStack)) {
                SchemaTable lastSchemaTable = distinctQueryStack.getLast().getSchemaTable();
                List<LinkedList<SchemaTableTree>> subQueryStacks = splitIntoSubStacks(distinctQueryStack);
                String singlePathSql = constructDuplicatePathSql(subQueryStacks);
                result.add(Pair.of(lastSchemaTable, singlePathSql));
            } else {
                //If there are no duplicates in the path then one select statement will suffice.
                SchemaTable lastSchemaTable = distinctQueryStack.getLast().getSchemaTable();
                String singlePathSql = constructSinglePathSql(distinctQueryStack, null, null);
                result.add(Pair.of(lastSchemaTable, singlePathSql));
            }
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
    private String constructDuplicatePathSql(List<LinkedList<SchemaTableTree>> subQueryLinkedLists) {
        String singlePathSql = "";
        singlePathSql += "SELECT ";
        singlePathSql += constructOuterFromClause(subQueryLinkedLists.get(subQueryLinkedLists.size() - 1).getLast().getSchemaTable(), subQueryLinkedLists.size());
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

            String sql = constructSinglePathSql(subQueryLinkedList, lastOfPrevious, firstOfNext);
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
            System.out.println(sql);
        }
        return singlePathSql;
    }

    private String constructOuterFromClause(SchemaTable schemaTable, int count) {
        String sql = "a" + count + ".\"" + schemaTable.getSchema() + "." + schemaTable.getTable() + "." + SchemaManager.ID + "\"";
        Map<String, PropertyType> propertyTypeMap = this.sqlgGraph.getSchemaManager().getLocalTables().get(schemaTable.toString());
        if (propertyTypeMap.size() > 0) {
            sql += ", ";
        }
        int propertyCount = 1;
        for (String propertyName : propertyTypeMap.keySet()) {
            sql += "a" + count + ".\"" + schemaTable.getSchema() + "." + schemaTable.getTable() + "." + propertyName + "\"";
            if (propertyCount < propertyTypeMap.size()) {
                sql += ",";
            }
        }
        return sql;
    }

    private String constructSectionedJoin(SchemaTableTree fromSchemaTableTree, SchemaTableTree toSchemaTableTree, int count) {
        if (toSchemaTableTree.direction == Direction.BOTH) {
            throw new IllegalStateException("Direction may not be BOTH!");
        }
        String result;
        if (fromSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.EDGE_PREFIX)) {
            if (toSchemaTableTree.direction == Direction.OUT) {
                result = "a" + (count - 1) + ".\"" + fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                        toSchemaTableTree.getSchemaTable().getSchema() + "." + toSchemaTableTree.getSchemaTable().getTable().replace(SchemaManager.VERTEX_PREFIX, "") + SchemaManager.IN_VERTEX_COLUMN_END + "\"";
                result += " = a" + count + ".\"" + toSchemaTableTree.getSchemaTable().getSchema() + "." + toSchemaTableTree.getSchemaTable().getTable() + "." + SchemaManager.ID + "\"";
            } else {
                result = "a" + (count - 1) + ".\"" + fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                        toSchemaTableTree.getSchemaTable().getSchema() + "." + toSchemaTableTree.getSchemaTable().getTable().replace(SchemaManager.VERTEX_PREFIX, "") + SchemaManager.OUT_VERTEX_COLUMN_END + "\"";
                result += " = a" + count + ".\"" + toSchemaTableTree.getSchemaTable().getSchema() + "." + toSchemaTableTree.getSchemaTable().getTable() + "." + SchemaManager.ID + "\"";
            }
        } else {
            if (toSchemaTableTree.direction == Direction.OUT) {
                result = "a" + (count - 1) + ".\"" + fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." + SchemaManager.ID + "\"";
                result += " = a" + count + ".\"" + toSchemaTableTree.getSchemaTable().getSchema() + "." + toSchemaTableTree.getSchemaTable().getTable() + "." +
                        fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable().replace(SchemaManager.VERTEX_PREFIX, "") + SchemaManager.OUT_VERTEX_COLUMN_END + "\"";
            } else {
                result = "a" + (count - 1) + ".\"" + fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." + SchemaManager.ID + "\"";
                result += " = a" + count + ".\"" + toSchemaTableTree.getSchemaTable().getSchema() + "." + toSchemaTableTree.getSchemaTable().getTable() + "." +
                        fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable().replace(SchemaManager.VERTEX_PREFIX, "") + SchemaManager.IN_VERTEX_COLUMN_END + "\"";
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
    private String constructSinglePathSql(LinkedList<SchemaTableTree> distinctQueryStack, SchemaTableTree lastOfPrevious, SchemaTableTree firstOfNextStack) {
        String singlePathSql = "";
        singlePathSql += "SELECT ";
        //first is last and last is first in a Stack
        SchemaTable firstSchemaTable = distinctQueryStack.getFirst().getSchemaTable();
        SchemaTable lastSchemaTable = distinctQueryStack.getLast().getSchemaTable();
        singlePathSql += constructFromClause(firstSchemaTable, lastSchemaTable, lastOfPrevious, firstOfNextStack);
        singlePathSql += " FROM ";
        SchemaTableTree firstSchemaTableTree = distinctQueryStack.getFirst();
        singlePathSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTableTree.getSchemaTable().getSchema());
        singlePathSql += ".";
        singlePathSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTableTree.getSchemaTable().getTable());
        SchemaTableTree previous = firstSchemaTableTree;
        boolean skipFirst = true;
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            if (skipFirst) {
                skipFirst = false;
                continue;
            }
            singlePathSql += constructJoinBetweenSchemaTables(schemaTableTree.direction, previous.getSchemaTable(), schemaTableTree.getSchemaTable());
            previous = schemaTableTree;
        }
        //lastOfPrevious is null for the first call in the call stack
        if (lastOfPrevious == null) {
            singlePathSql += " WHERE ";
            singlePathSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema());
            singlePathSql += ".";
            singlePathSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable());
            singlePathSql += "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
            singlePathSql += " = ? ";
        }
        return singlePathSql;
    }

    private List<LinkedList<SchemaTableTree>> splitIntoSubStacks(LinkedList<SchemaTableTree> distinctQueryStack) {
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
    private boolean duplicatesInStack(LinkedList<SchemaTableTree> distinctQueryStack) {
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
     * Constructs the from clause with the required selected fields needed to make the join between the from and the toSchemaTable
     *
     * @param firstSchemaTable        This is the first SchemaTable in the current sql stack. If it is an Edge table then its foreign key
     *                                field to the previous table needs to in the select clause in order for the join statement to
     *                                reference it.
     * @param lastSchemaTable         This is the label that
     * @param previousSchemaTableTree
     * @param nextSchemaTableTree     represents the table to join to. it is null for the last table as there is nothing to join to.  @return
     */
    private String constructFromClause(SchemaTable firstSchemaTable, SchemaTable lastSchemaTable, SchemaTableTree previousSchemaTableTree, SchemaTableTree nextSchemaTableTree) {
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

        if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(SchemaManager.EDGE_PREFIX)) {
            if (previousSchemaTableTree.direction == Direction.OUT) {
                sql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()) + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()) + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                previousSchemaTableTree.getSchemaTable().getSchema() + "." +
                                        previousSchemaTableTree.getSchemaTable().getTable().replace(SchemaManager.VERTEX_PREFIX, "") + SchemaManager.IN_VERTEX_COLUMN_END
                        );
                sql += " AS \"" + firstSchemaTable.getSchema() + "." + firstSchemaTable.getTable() + "." +
                        previousSchemaTableTree.getSchemaTable().getSchema() + "." +
                        previousSchemaTableTree.getSchemaTable().getTable().replace(SchemaManager.VERTEX_PREFIX, "") + SchemaManager.IN_VERTEX_COLUMN_END + "\"";
            } else {
                sql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()) + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()) + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                previousSchemaTableTree.getSchemaTable().getSchema() + "." +
                                        previousSchemaTableTree.getSchemaTable().getTable().replace(SchemaManager.VERTEX_PREFIX, "") + SchemaManager.OUT_VERTEX_COLUMN_END
                        );
                sql += " AS \"" + firstSchemaTable.getSchema() + "." + firstSchemaTable.getTable() + "." +
                        previousSchemaTableTree.getSchemaTable().getSchema() + "." +
                        previousSchemaTableTree.getSchemaTable().getTable().replace(SchemaManager.VERTEX_PREFIX, "") + SchemaManager.OUT_VERTEX_COLUMN_END + "\"";
            }
        } else if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            sql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()) + "." +
                    this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()) + "." +
                    this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
            sql += " AS \"" + firstSchemaTable.getSchema() + "." + firstSchemaTable.getTable() + "." + SchemaManager.ID + "\"";
            printedId = firstSchemaTable == lastSchemaTable;
        }

        if (nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(SchemaManager.EDGE_PREFIX)) {
            if (!sql.isEmpty()) {
                sql += ", ";
            }
            if (nextSchemaTableTree.direction == Direction.OUT) {
                sql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getSchema()) + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getTable()) + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                        nextSchemaTableTree.getSchemaTable().getTable().replace(SchemaManager.VERTEX_PREFIX, "") + SchemaManager.IN_VERTEX_COLUMN_END
                        );
                sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                        nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                        nextSchemaTableTree.getSchemaTable().getTable().replace(SchemaManager.VERTEX_PREFIX, "") + SchemaManager.IN_VERTEX_COLUMN_END + "\"";
            } else {
                sql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getSchema()) + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getTable()) + "." +
                        this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                        nextSchemaTableTree.getSchemaTable().getTable().replace(SchemaManager.VERTEX_PREFIX, "") + SchemaManager.OUT_VERTEX_COLUMN_END
                        );
                sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                        nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                        nextSchemaTableTree.getSchemaTable().getTable().replace(SchemaManager.VERTEX_PREFIX, "") + SchemaManager.OUT_VERTEX_COLUMN_END + "\"";
            }
        } else if (nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            if (!sql.isEmpty()) {
                sql += ", ";
            }
            sql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getSchema()) + "." +
                    this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getTable()) + "." +
                    this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
            sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." + SchemaManager.ID + "\"";
            printedId = firstSchemaTable == lastSchemaTable;
        }

        //The last schemaTableTree in the call stack as no nextSchemaTableTree.
        //This last element's properties need to be returned.
        if (nextSchemaTableTree == null) {
            if (!sql.isEmpty()) {
                sql += ", ";
            }
            String finalFromSchemaTableName = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getSchema());
            finalFromSchemaTableName += ".";
            finalFromSchemaTableName += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getTable());

            Map<String, PropertyType> propertyTypeMap = this.sqlgGraph.getSchemaManager().getLocalTables().get(lastSchemaTable.toString());
            if (!printedId) {
                sql += finalFromSchemaTableName + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
                sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." + SchemaManager.ID + "\"";
                if (propertyTypeMap.size() > 0) {
                    sql += ", ";
                }
            }
            int propertyCount = 1;
            for (String propertyName : propertyTypeMap.keySet()) {
                sql += finalFromSchemaTableName + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(propertyName);
                sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." + propertyName + "\"";
                if (propertyCount < propertyTypeMap.size()) {
                    sql += ",";
                }
            }
        }
        return sql;
    }

    private LinkedList<SchemaTableTree> constructQueryStackFromLeaf(SchemaTableTree leafNode) {
        LinkedList<SchemaTableTree> queryCallStack = new LinkedList<>();
        SchemaTableTree node = leafNode;
        while (node != null) {
            queryCallStack.add(0, node);
            node = node.parent;
        }
        return queryCallStack;
    }

    private String constructJoinBetweenSchemaTables(Direction direction, SchemaTable schemaTable, SchemaTable labelToTravers) {
        String joinSql = " INNER JOIN ";
        if (schemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
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
        } else {
            joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema());
            joinSql += ".";
            joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable());
            joinSql += " ON ";
            joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema());
            joinSql += ".";
            joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getTable());
            joinSql += ".";
            joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema() + "." +
                    labelToTravers.getTable().replace(SchemaManager.VERTEX_PREFIX, "") + (direction == Direction.OUT ? SchemaManager.IN_VERTEX_COLUMN_END : SchemaManager.OUT_VERTEX_COLUMN_END));
            joinSql += " = ";
            joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema());
            joinSql += ".";
            joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable());
            joinSql += ".";
            joinSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID");
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
            }
            queue.addAll(current.children);
            if (current.stepDepth == depth && current.children.isEmpty()) {
                this.leafNodes.add(current);
            }
        }
    }

    private void removeNode(SchemaTableTree node) {
        SchemaTableTree parent = node.parent;
        parent.children.remove(node);
        //check if the parent has any other children. if not it too can be deleted. Follow this pattern recursively up.
        if (parent.children.isEmpty()) {
            removeNode(parent);
        }
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
        sb.append(this.schemaTable.toString()).append(" ").append(this.stepDepth);
        for (SchemaTableTree child : children) {
            child.internalToString(sb);
        }
    }
}
