package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;

import java.util.*;

/**
 * Date: 2015/11/02
 * Time: 8:20 AM
 */
public final class AliasMapHolder implements Cloneable {

    private Map<String, String> columnNameAliasMap;
    private Map<String, String> aliasColumnNameMap;
    private List<ColumnList> columnListStack;
    private final Map<SchemaTableTree, List<LinkedHashMap<ColumnList.Column, String>>> columnsMap = new HashMap<>();

    AliasMapHolder() {
        this.columnNameAliasMap = new HashMap<>();
        this.aliasColumnNameMap = new HashMap<>();
        this.columnListStack = new ArrayList<>();
    }

    Map<String, String> getColumnNameAliasMap() {
        return columnNameAliasMap;
    }

    Map<String, String> getAliasColumnNameMap() {
        return aliasColumnNameMap;
    }

    List<ColumnList> getColumnListStack() {
        return columnListStack;
    }

    public List<LinkedHashMap<ColumnList.Column, String>> getColumns(SchemaTableTree schemaTableTree) {
        return this.columnsMap.get(schemaTableTree);
    }

    public void calculateColumns(List<LinkedList<SchemaTableTree>> subQueryStack) {
        Preconditions.checkState(this.columnsMap.isEmpty());
        for (LinkedList<SchemaTableTree> schemaTableTrees : subQueryStack) {
            for (SchemaTableTree schemaTableTree : schemaTableTrees) {
                for (ColumnList columnList : this.columnListStack) {
                    LinkedHashMap<ColumnList.Column, String> columns = columnList.getFor(schemaTableTree.getStepDepth(), schemaTableTree.getSchemaTable());
                    this.columnsMap.computeIfAbsent(schemaTableTree, (k) -> new ArrayList<>()).add(columns);
                }
            }
        }
    }

    void clear() {
        this.columnNameAliasMap.clear();
        this.aliasColumnNameMap.clear();
        this.columnListStack.clear();
        this.columnsMap.clear();
    }

    @Override
    public AliasMapHolder clone() {
        try {
            AliasMapHolder clone = (AliasMapHolder) super.clone();
            clone.columnNameAliasMap = new HashMap<>(this.columnNameAliasMap);
            clone.aliasColumnNameMap = new HashMap<>(this.aliasColumnNameMap);
            clone.columnListStack = new ArrayList<>(this.columnListStack);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
