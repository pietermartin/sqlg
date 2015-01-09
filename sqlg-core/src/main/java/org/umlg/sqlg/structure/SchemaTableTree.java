package org.umlg.sqlg.structure;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Date: 2015/01/08
 * Time: 7:06 AM
 */
public class SchemaTableTree {
    private int depth;
    private SchemaTable schemaTable;
    private SchemaTableTree parent;
    private List<SchemaTableTree> children = new ArrayList<>();

    SchemaTableTree(SchemaTable schemaTable, int depth) {
        this.schemaTable = schemaTable;
        this.depth = depth;
    }

    SchemaTableTree addChild(SchemaTable schemaTable, int depth) {
        SchemaTableTree schemaTableTree = new SchemaTableTree(schemaTable, depth);
        schemaTableTree.parent = this;
        this.children.add(schemaTableTree);
        return schemaTableTree;
    }

    SchemaTable getSchemaTable() {
        return schemaTable;
    }

    public String constructSql() {
        return null;
    }

    /**
     * Remove all leaf nodes that are not at the deepest level.
     * Those nodes are not to be included in the sql as they do not have enough incident edges.
     * i.e. The graph is not deep enough along those labels.
     * <p>
     * This is done via a breath first traversal.
     */
    public void postParse(int depth) {
        Queue<SchemaTableTree> queue = new LinkedList<>();
        queue.add(this);
        while (!queue.isEmpty()) {
            SchemaTableTree current = queue.remove();
            if (current.depth < depth && current.children.isEmpty()) {
                internalPostParseRemoveNode(current);
            }
            queue.addAll(current.children);
        }
    }

    private void internalPostParseRemoveNode(SchemaTableTree node) {

        SchemaTableTree parent = node.parent;
        parent.children.remove(node);
        //check if the parent has any other children. if not it too can be deleted. Follow this pattern recursively up.
        if (parent.children.isEmpty()){
            internalPostParseRemoveNode(parent);
        }

    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        internalToString(result);
        return result.toString();
    }

    private void internalToString(StringBuilder sb) {
        if (sb.length() > 0) {
            sb.append("\n");
        }
        for (int i = 0; i < this.depth; i++) {
            sb.append("\t");
        }
        sb.append(this.schemaTable.toString()).append(" ").append(this.depth);
        for (SchemaTableTree child : children) {
            child.internalToString(sb);
        }
    }
}
