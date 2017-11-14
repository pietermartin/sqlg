package org.umlg.sqlg.mssqlserver;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.apache.commons.lang3.tuple.Pair;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.GlobalUniqueIndex;
import org.umlg.sqlg.structure.topology.PropertyColumn;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/08/06
 */
public class SQLServerVertexGlobalUniqueIndexBulkRecord extends SQLServerBaseCacheBulkRecord implements ISQLServerBulkRecord {

    private Iterator<Map.Entry<SqlgVertex, Map<String, Object>>> rowIter;
    private Map<String, Object> currentRow;
    private SqlgVertex currentVertex;
    private PropertyColumn propertyColumn;

    SQLServerVertexGlobalUniqueIndexBulkRecord(SQLServerBulkCopy bulkCopy,
                                               SqlgGraph sqlgGraph,
                                               Pair<SortedSet<String>,
                                                 Map<SqlgVertex, Map<String, Object>>> vertices,
                                               PropertyColumn propertyColumn
    ) throws SQLServerException {
        this.rowIter = vertices.getRight().entrySet().iterator();
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
        Object value = this.currentRow.get(this.propertyColumn.getName());
        if (value == null) {
            value = "dummy_" + UUID.randomUUID().toString();
        }
        values.add(value);
        values.add(this.currentVertex.id().toString());
        values.add(this.propertyColumn.getName());
        return values.toArray();
    }

    Object getValue(String column) {
        return this.currentRow.get(column);
    }

    @Override
    public boolean next() throws SQLServerException {
        if (this.rowIter.hasNext()) {
            Map.Entry<SqlgVertex, Map<String, Object>> entry = this.rowIter.next();
            this.currentRow = entry.getValue();
            this.currentVertex = entry.getKey();
            return true;
        } else {
            return false;
        }
    }
}
