package org.umlg.sqlg.sql.parse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 2015/11/02
 * Time: 8:20 AM
 */
public final class AliasMapHolder implements Cloneable {

    private Map<String, String> columnNameAliasMap;
    private Map<String, String> aliasColumnNameMap;
    private List<ColumnList> columnListStack;

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

    void clear() {
        this.columnNameAliasMap.clear();
        this.aliasColumnNameMap.clear();
        this.columnListStack.clear();
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
