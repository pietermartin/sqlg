package org.umlg.sqlg.sql.parse;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2015/11/02
 * Time: 8:20 AM
 */
public class AliasMapHolder {

    private Multimap<String, String> columnNameAliasMap;
    private Map<String, String> aliasColumnNameMap;

    AliasMapHolder() {
        this.columnNameAliasMap = ArrayListMultimap.create();
        this.aliasColumnNameMap = new HashMap<>();
    }

    Multimap<String, String> getColumnNameAliasMap() {
        return columnNameAliasMap;
    }

    Map<String, String> getAliasColumnNameMap() {
        return aliasColumnNameMap;
    }

    public AliasMapHolder copy() {
        AliasMapHolder copy = new AliasMapHolder();
        for (String key : this.columnNameAliasMap.keySet()) {
            Collection<String> values = this.columnNameAliasMap.get(key);
            for (String value : values) {
                copy.columnNameAliasMap.put(key, value);
            }
        }
        copy.aliasColumnNameMap.putAll(this.aliasColumnNameMap);
        return copy;
    }

    void clear() {
        this.columnNameAliasMap.clear();
        this.aliasColumnNameMap.clear();
    }

}
