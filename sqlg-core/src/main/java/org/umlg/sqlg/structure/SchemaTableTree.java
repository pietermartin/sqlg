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
        List<Stack<SchemaTableTree>> distinctQueries = new ArrayList<>();
        for (SchemaTableTree leafNode : this.leafNodes) {
            distinctQueries.add(constructQueryStackFromLeaf(leafNode));
        }
        for (Stack<SchemaTableTree> distinctQueryStack : distinctQueries) {

            //If the same element occurs multiple times in the stack then the sql needs to be different.
            //This is because the same element can not be joined on more than once in sql
            //The way to overcome this is  to break up the path in select sections with no duplicates and then join them together.
            if (duplicatesInStack(distinctQueryStack)) {
                SchemaTable firstSchemaTable = distinctQueryStack.firstElement().getSchemaTable();
                List<Stack<SchemaTableTree>> subQueryStacks = splitIntoSubStacks(distinctQueryStack);
                String singlePathSql = constructDuplicatePathSql(subQueryStacks);
                result.add(Pair.of(firstSchemaTable, singlePathSql));
            } else {
                //If there are no duplicates in the path then one select statement will suffice.
                SchemaTable firstSchemaTable = distinctQueryStack.firstElement().getSchemaTable();
                String singlePathSql = constructSinglePathSql(true, true, distinctQueryStack, null);
                result.add(Pair.of(firstSchemaTable, singlePathSql));
            }
        }
        return result;
    }

    private String constructDuplicatePathSql(List<Stack<SchemaTableTree>> subQueryStacks) {
        String singlePathSql = "";
        singlePathSql += "SELECT * ";
        singlePathSql += " FROM ";
        int count = 1;
        for (Stack<SchemaTableTree> subQueryStack : subQueryStacks) {
            boolean last = count == subQueryStacks.size();
            SchemaTableTree toJoinOnNext = null;
            if (!last) {
                Stack<SchemaTableTree> nextStack = subQueryStacks.get(count);
                SchemaTableTree firstEqualsLastFromPrevious = nextStack.pop();
                toJoinOnNext = nextStack.pop();
                nextStack.push(toJoinOnNext);
                nextStack.push(firstEqualsLastFromPrevious);
            }
            String sql = constructSinglePathSql(count++ == 1, last, subQueryStack, toJoinOnNext);
            System.out.println(sql);
        }
        return singlePathSql;
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
     * @param first              true if its the first call stack in the query
     * @param last               true if its the last call stack in the query
     * @param distinctQueryStack
     * @return
     */
    private String constructSinglePathSql(boolean first, boolean last, Stack<SchemaTableTree> distinctQueryStack, SchemaTableTree firstOfNextStack) {
        String singlePathSql = "";
        singlePathSql += "SELECT ";
        //first is last and last is first in a Stack
        SchemaTable firstSchemaTable = distinctQueryStack.lastElement().getSchemaTable();
        SchemaTable lastSchemaTable = distinctQueryStack.firstElement().getSchemaTable();
        singlePathSql += constructFromClause(first, last, firstSchemaTable, lastSchemaTable, firstOfNextStack);
        singlePathSql += " FROM ";
        SchemaTableTree schemaTableTree = distinctQueryStack.pop();
        singlePathSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTableTree.getSchemaTable().getSchema());
        singlePathSql += ".";
        singlePathSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTableTree.getSchemaTable().getTable());
        SchemaTableTree previous = schemaTableTree;
        while (!distinctQueryStack.isEmpty()) {
            schemaTableTree = distinctQueryStack.pop();
            singlePathSql += constructJoinBetweenSchemaTables(schemaTableTree.direction, previous.getSchemaTable(), schemaTableTree.getSchemaTable());
            previous = schemaTableTree;
        }
        singlePathSql += " WHERE ";
        singlePathSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema());
        singlePathSql += ".";
        singlePathSql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable());
        singlePathSql += "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
        singlePathSql += " = ?";
        return singlePathSql;
    }

    private List<Stack<SchemaTableTree>> splitIntoSubStacks(Stack<SchemaTableTree> distinctQueryStack) {
        List<Stack<SchemaTableTree>> result = new ArrayList<>();
        Stack<SchemaTableTree> subStack = new Stack<>();
        result.add(subStack);
        //iterate the stack, that iterates the stack in reverse order
        SchemaTableTree previous = null;
        Set<SchemaTable> alreadyVisited = new HashSet<>();
        while (!distinctQueryStack.isEmpty()) {
            SchemaTableTree schemaTableTree = distinctQueryStack.pop();
            if (!alreadyVisited.contains(schemaTableTree.getSchemaTable())) {
                alreadyVisited.add(schemaTableTree.getSchemaTable());
                subStack.add(0, schemaTableTree);
            } else {
                subStack = new Stack<>();
                subStack.add(0, previous);
                subStack.add(0, schemaTableTree);
                result.add(subStack);
            }
            previous = schemaTableTree;
        }
        return result;
    }

    /**
     * Checks if the stack has the same element more than once.
     *
     * @param distinctQueryStack
     * @return true is there are duplicates else false
     */
    private boolean duplicatesInStack(Stack<SchemaTableTree> distinctQueryStack) {
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

    private String constructFromClause(boolean first, boolean last, SchemaTable firstSchemaTable, SchemaTable lastSchemaTable, SchemaTableTree toJoinOnNext) {
        String sql;
        //no duplicate labels in the gremlin. i.e. return only the lastSchemaTable fields
        if (first && last) {
            if (toJoinOnNext != null) {
                throw new IllegalStateException("toJoinOnNext SchemaTable must be null for a gremlin query with no duplicate labels: BUG");
            }
            sql = constructFromClause(lastSchemaTable, null);
        } else if (first && !last) {
            if (toJoinOnNext == null) {
                throw new IllegalStateException("toJoinOnNext SchemaTable may not be null for a gremlin query with duplicate labels: BUG");
            }
            sql = constructFromClause(lastSchemaTable, toJoinOnNext);
        } else if (!first && last) {
            if (toJoinOnNext != null) {
                throw new IllegalStateException("toJoinOnNext SchemaTable may not be null for a gremlin query with duplicate labels: BUG");
            }
            sql = constructFromClause(lastSchemaTable, null);
        } else if (!first && !last) {
            if (toJoinOnNext == null) {
                throw new IllegalStateException("toJoinOnNext SchemaTable may not be null for a gremlin query with duplicate labels: BUG");
            }
            sql = constructFromClause(lastSchemaTable, toJoinOnNext);
        } else {
            throw new RuntimeException("asdasd");
        }
        return sql;
    }

    private String constructFromClause(SchemaTable lastSchemaTable, SchemaTableTree toJoinOnNext) {
        String finalSchemaTableName = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getSchema());
        finalSchemaTableName += ".";
        finalSchemaTableName += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getTable());
        String sql;
        if (lastSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            sql = finalSchemaTableName + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
            sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." + SchemaManager.ID + "\"";
        } else {
            if (toJoinOnNext.direction == Direction.OUT) {
                sql = finalSchemaTableName + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                        toJoinOnNext.getSchemaTable().getSchema() + "." + toJoinOnNext.getSchemaTable().getTable().replace(SchemaManager.VERTEX_PREFIX, "") + SchemaManager.IN_VERTEX_COLUMN_END);
                sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                        toJoinOnNext.getSchemaTable().getSchema() + "." + toJoinOnNext.getSchemaTable().getTable().replace(SchemaManager.VERTEX_PREFIX, "") + SchemaManager.IN_VERTEX_COLUMN_END + "\"";
            } else if (toJoinOnNext.direction == Direction.IN) {
                sql = finalSchemaTableName + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                        toJoinOnNext.getSchemaTable().getTable().replace(SchemaManager.VERTEX_PREFIX, "") + SchemaManager.OUT_VERTEX_COLUMN_END);
                sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                        toJoinOnNext.getSchemaTable().getTable().replace(SchemaManager.VERTEX_PREFIX, "") + SchemaManager.OUT_VERTEX_COLUMN_END + "\"";
            } else {
                throw new IllegalStateException("Direction should never be BOTH");
            }
        }
        Map<String, PropertyType> propertyTypeMap = this.sqlgGraph.getSchemaManager().getLocalTables().get(lastSchemaTable.toString());
        if (propertyTypeMap.size() > 0) {
            sql += ", ";
        }
        int propertyCount = 1;
        for (String propertyName : propertyTypeMap.keySet()) {
            sql += finalSchemaTableName + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(propertyName);
            sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." + propertyName + "\"";
            if (propertyCount < propertyTypeMap.size()) {
                sql += ",";
            }
        }
        return sql;
    }

    private Stack constructQueryStackFromLeaf(SchemaTableTree leafNode) {
        Stack queryCallStack = new Stack();
        SchemaTableTree node = leafNode;
        while (node != null) {
            queryCallStack.push(node);
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
