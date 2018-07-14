package org.umlg.sqlg.sql.parse;

import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2015/11/02
 * Time: 8:20 AM
 */
class AliasMapHolder {

    private final Map<String, String> columnNameAliasMap;
    private final Map<String, String> aliasColumnNameMap;

    AliasMapHolder() {
        this.columnNameAliasMap = new HashMap<>();
        this.aliasColumnNameMap = new HashMap<>();
    }

    Map<String, String> getColumnNameAliasMap() {
        return columnNameAliasMap;
    }

    Map<String, String> getAliasColumnNameMap() {
        return aliasColumnNameMap;
    }

    void clear() {
        this.columnNameAliasMap.clear();
        this.aliasColumnNameMap.clear();
    }

}
