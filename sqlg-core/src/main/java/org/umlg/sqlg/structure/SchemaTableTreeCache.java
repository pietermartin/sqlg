package org.umlg.sqlg.structure;

import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.AliasMapHolder;
import org.umlg.sqlg.sql.parse.SchemaTableTree;

import java.util.LinkedList;

public final class SchemaTableTreeCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaTableTreeCache.class);
    private final LRUMap<Pair<SchemaTableTree, LinkedList<SchemaTableTree>>, Pair<String, AliasMapHolder>> lruMap;

    SchemaTableTreeCache(int cacheSize) {
        this.lruMap = new LRUMap<>(cacheSize);
    }

    private void put(Pair<SchemaTableTree, LinkedList<SchemaTableTree>> key, Pair<String, AliasMapHolder> value) {
        if (this.lruMap.isFull()) {
            LOGGER.debug("LRUMap is full!!!");
        }
        this.lruMap.put(key, value);
    }

    private Pair<String, AliasMapHolder> get(Pair<SchemaTableTree, LinkedList<SchemaTableTree>> key) {
        return this.lruMap.get(key);
    }

    public void clear() {
        synchronized (this.lruMap) {
            this.lruMap.clear();
        }
    }

    public String sql(Pair<SchemaTableTree, LinkedList<SchemaTableTree>> p) {
        SchemaTableTree rootSchemaTableTree = p.getLeft();
        LinkedList<SchemaTableTree> distinctQueryStack = p.getRight();
        String sql;
        //We do not cache bulk within as the sql has a `VALUES` clause.
        if (distinctQueryStack.stream().anyMatch(SchemaTableTree::hasBulkWithinOrOut)) {
            sql = rootSchemaTableTree.constructSql(distinctQueryStack);
        } else {
            Pair<String, AliasMapHolder> sqlAliasMapHolder = get(p);
            if (sqlAliasMapHolder == null) {
                synchronized (this.lruMap) {
                    sqlAliasMapHolder = get(p);
                    if (sqlAliasMapHolder == null) {
                        sql = rootSchemaTableTree.constructSql(distinctQueryStack);
                        put(p, Pair.of(sql, rootSchemaTableTree.getAliasMapHolder().clone()));
                    } else {
                        sql = sqlAliasMapHolder.getKey();
                        rootSchemaTableTree.setAliasMapHolder(sqlAliasMapHolder.getValue().clone());
                    }
                }
            } else {
                sql = sqlAliasMapHolder.getKey();
                rootSchemaTableTree.setAliasMapHolder(sqlAliasMapHolder.getValue().clone());
            }
        }
        return sql;
    }

}
