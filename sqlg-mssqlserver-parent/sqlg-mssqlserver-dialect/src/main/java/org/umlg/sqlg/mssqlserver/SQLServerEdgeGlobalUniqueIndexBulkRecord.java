package org.umlg.sqlg.mssqlserver;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.apache.commons.lang3.tuple.Triple;
import org.umlg.sqlg.structure.*;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/08/06
 */
public class SQLServerEdgeGlobalUniqueIndexBulkRecord extends SQLServerBaseCacheBulkRecord implements ISQLServerBulkRecord {

    private Iterator<Map.Entry<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> rowIter;
    private Triple<SqlgVertex, SqlgVertex, Map<String, Object>> currentRow;
    private SqlgEdge currentEdge;
    private PropertyColumn propertyColumn;

    SQLServerEdgeGlobalUniqueIndexBulkRecord(SQLServerBulkCopy bulkCopy,
                                             SqlgGraph sqlgGraph,
                                             Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> edgeMap,
                                             PropertyColumn propertyColumn
    ) throws SQLServerException {

        this.rowIter = edgeMap.entrySet().iterator();
        this.propertyColumn = propertyColumn;

        bulkCopy.addColumnMapping(1, GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE);
        this.columnMetadata.put(1, new ColumnMetadata(
                GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE,
                sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(PropertyType.STRING)[0],
                0,
                0,
                null,
                PropertyType.STRING
        ));
        bulkCopy.addColumnMapping(2, GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_RECORD_ID);
        this.columnMetadata.put(2, new ColumnMetadata(
                GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_RECORD_ID,
                sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(PropertyType.STRING)[0],
                0,
                0,
                null,
                PropertyType.STRING
        ));
        bulkCopy.addColumnMapping(3, GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_PROPERTY_NAME);
        this.columnMetadata.put(3, new ColumnMetadata(
                GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_PROPERTY_NAME,
                sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(PropertyType.STRING)[0],
                0,
                0,
                null,
                PropertyType.STRING
        ));
    }

    @Override
    public Object[] getRowData() throws SQLServerException {
        List<Object> values = new ArrayList<>();
        Object value = this.currentRow.getRight().get(this.propertyColumn.getName());
        if (value == null) {
            value = "dummy_" + UUID.randomUUID().toString();
        }
        values.add(value);
        values.add(this.currentEdge.id().toString());
        values.add(this.propertyColumn.getName());
        return values.toArray();
    }

    Object getValue(String column) {
        return this.currentRow.getRight().get(column);
    }

    @Override
    public boolean next() throws SQLServerException {
        if (this.rowIter.hasNext()) {
            Map.Entry<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> entry = this.rowIter.next();
            this.currentRow = entry.getValue();
            this.currentEdge = entry.getKey();
            return true;
        } else {
            return false;
        }
    }
}
