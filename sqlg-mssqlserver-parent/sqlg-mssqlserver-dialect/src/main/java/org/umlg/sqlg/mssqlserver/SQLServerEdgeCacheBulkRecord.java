package org.umlg.sqlg.mssqlserver;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.Topology;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/08/06
 */
public class SQLServerEdgeCacheBulkRecord extends SQLServerBaseCacheBulkRecord implements ISQLServerBulkRecord {

    private Iterator<Map.Entry<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> rowIter;
    private Triple<SqlgVertex, SqlgVertex, Map<String, Object>> currentRow;

    SQLServerEdgeCacheBulkRecord(SQLServerBulkCopy bulkCopy, SqlgGraph sqlgGraph, MetaEdge metaEdge, SchemaTable schemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> triples) throws SQLServerException {
        this.rowIter = triples.getRight().entrySet().iterator();
        this.propertyColumns = sqlgGraph.getTopology()
                .getSchema(schemaTable.getSchema()).orElseThrow(() -> new IllegalStateException(String.format("Schema '%s' not found", schemaTable.getSchema())))
                .getEdgeLabel(schemaTable.getTable()).orElseThrow(() -> new IllegalStateException(String.format("EdgeLabel '%s' not found", schemaTable.getTable())))
                .getProperties();

        this.columns = triples.getLeft();
        int i = addMetaData(bulkCopy, sqlgGraph);

        bulkCopy.addColumnMapping(i, metaEdge.getInLabel() + Topology.IN_VERTEX_COLUMN_END);
        this.columnMetadata.put(i++, new ColumnMetadata(
                metaEdge.getInLabel() + Topology.IN_VERTEX_COLUMN_END,
                sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(PropertyType.LONG)[0],
                0,
                0,
                null,
                PropertyType.LONG
        ));
        bulkCopy.addColumnMapping(i, metaEdge.getOutLabel() + Topology.OUT_VERTEX_COLUMN_END);
        this.columnMetadata.put(i, new ColumnMetadata(
                metaEdge.getOutLabel() + Topology.OUT_VERTEX_COLUMN_END,
                sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(PropertyType.LONG)[0],
                0,
                0,
                null,
                PropertyType.LONG
        ));
    }

    Object getValue(String column) {
        return this.currentRow.getRight().get(column);
    }
    @Override
    public Object[] getRowData() throws SQLServerException {
        List<Object> values = new ArrayList<>();
        addValues(values);
        values.add(((RecordId) this.currentRow.getMiddle().id()).getId());
        values.add(((RecordId) this.currentRow.getLeft().id()).getId());
        return values.toArray();
    }

    @Override
    public boolean next() throws SQLServerException {
        if (this.rowIter.hasNext()) {
            Map.Entry<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> entry = this.rowIter.next();
            this.currentRow = entry.getValue();
            return true;
        } else {
            return false;
        }
    }
}
