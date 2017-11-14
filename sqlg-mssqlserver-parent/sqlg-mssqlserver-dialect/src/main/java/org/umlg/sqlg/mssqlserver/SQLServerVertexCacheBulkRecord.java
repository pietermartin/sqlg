package org.umlg.sqlg.mssqlserver;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.apache.commons.lang3.tuple.Pair;
import org.umlg.sqlg.structure.*;

import java.util.*;

import static org.umlg.sqlg.structure.topology.Topology.VERTEX_PREFIX;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/08/06
 */
public class SQLServerVertexCacheBulkRecord extends SQLServerBaseCacheBulkRecord implements ISQLServerBulkRecord {

    private Iterator<Map.Entry<SqlgVertex, Map<String, Object>>> rowIter;
    private Map<String, Object> currentRow;
    private boolean dummy;

    SQLServerVertexCacheBulkRecord(SQLServerBulkCopy bulkCopy, SqlgGraph sqlgGraph, SchemaTable schemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>> vertices) throws SQLServerException {
        this.rowIter = vertices.getRight().entrySet().iterator();
        if (!schemaTable.isTemporary()) {
            this.propertyColumns = sqlgGraph.getTopology()
                    .getSchema(schemaTable.getSchema()).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found", schemaTable.getSchema())))
                    .getVertexLabel(schemaTable.getTable()).orElseThrow(() -> new IllegalStateException(String.format("VertexLabel %s not found", schemaTable.getTable())))
                    .getProperties();
        } else {
            this.properties = sqlgGraph.getTopology().getPublicSchema().getTemporaryTable(VERTEX_PREFIX + schemaTable.getTable());
        }
        int i = 1;
        this.columns = vertices.getLeft();
        this.dummy = this.columns.isEmpty();
        if (this.dummy) {
            bulkCopy.addColumnMapping(i, "dummy");
            this.columnMetadata.put(i, new ColumnMetadata(
                    "dummy",
                    sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(PropertyType.INTEGER)[0],
                    0,
                    0,
                    null,
                    PropertyType.INTEGER
            ));
        } else {
            addMetaData(bulkCopy, sqlgGraph);
        }
    }

    @Override
    public Object[] getRowData() throws SQLServerException {
        if (this.dummy) {
            return new Object[]{1};
        } else {
            List<Object> values = new ArrayList<>();
            addValues(values);
            return values.toArray();
        }
    }

    Object getValue(String column) {
        return this.currentRow.get(column);
    }

    @Override
    public boolean next() throws SQLServerException {
        if (this.rowIter.hasNext()) {
            Map.Entry<SqlgVertex, Map<String, Object>> entry = this.rowIter.next();
            this.currentRow = entry.getValue();
            return true;
        } else {
            return false;
        }
    }
}
