package org.umlg.sqlg.mssqlserver;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.structure.topology.VertexLabel;

import java.util.*;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2017/08/06
 */
class SQLServerEdgeCacheBulkRecord extends SQLServerBaseCacheBulkRecord implements ISQLServerBulkRecord {

    private final Iterator<Map.Entry<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> rowIter;
    private Triple<SqlgVertex, SqlgVertex, Map<String, Object>> currentRow;
    private final VertexLabel outVertexLabel;
    private final VertexLabel inVertexLabel;

    SQLServerEdgeCacheBulkRecord(SQLServerBulkCopy bulkCopy, SqlgGraph sqlgGraph, MetaEdge metaEdge, SchemaTable schemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> triples) throws SQLServerException {
        this.rowIter = triples.getRight().entrySet().iterator();

        SchemaTable outSchemaTable = SchemaTable.from(sqlgGraph, metaEdge.getOutLabel());
        SchemaTable inSchemaTable = SchemaTable.from(sqlgGraph, metaEdge.getInLabel());
        this.outVertexLabel = sqlgGraph.getTopology().getVertexLabel(outSchemaTable.getSchema(), outSchemaTable.getTable()).orElseThrow(() -> new IllegalStateException(String.format("VertexLabel not found for %s.%s", outSchemaTable.getSchema(), outSchemaTable.getTable())));
        this.inVertexLabel = sqlgGraph.getTopology().getVertexLabel(inSchemaTable.getSchema(), inSchemaTable.getTable()).orElseThrow(() -> new IllegalStateException(String.format("VertexLabel not found for %s.%s", inSchemaTable.getSchema(), inSchemaTable.getTable())));

        this.propertyColumns = sqlgGraph.getTopology()
                .getSchema(schemaTable.getSchema()).orElseThrow(() -> new IllegalStateException(String.format("Schema '%s' not found", schemaTable.getSchema())))
                .getEdgeLabel(schemaTable.getTable()).orElseThrow(() -> new IllegalStateException(String.format("EdgeLabel '%s' not found", schemaTable.getTable())))
                .getProperties();

        this.columns = triples.getLeft();
        int i = addMetaData(bulkCopy, sqlgGraph);

        if (inVertexLabel.hasIDPrimaryKey()) {
            bulkCopy.addColumnMapping(i, metaEdge.getInLabel() + Topology.IN_VERTEX_COLUMN_END);
            this.columnMetadata.put(i++, new ColumnMetadata(
                    metaEdge.getInLabel() + Topology.IN_VERTEX_COLUMN_END,
                    sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(PropertyType.LONG)[0],
                    0,
                    0,
                    null,
                    PropertyType.LONG
            ));
        } else {
            for (String identifier : inVertexLabel.getIdentifiers()) {
                bulkCopy.addColumnMapping(i, metaEdge.getInLabel() + "." + identifier + Topology.IN_VERTEX_COLUMN_END);
                PropertyType propertyType = inVertexLabel.getProperty(identifier).orElseThrow(() -> new IllegalStateException(String.format("BUG: Did not find the identifier property %s.", identifier))).getPropertyType();
                this.columnMetadata.put(i++, new ColumnMetadata(
                        metaEdge.getInLabel() + "." + identifier + Topology.IN_VERTEX_COLUMN_END,
                        sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(propertyType)[0],
                        0,
                        0,
                        null,
                        propertyType
                ));
            }
        }
        if (outVertexLabel.hasIDPrimaryKey()) {
            bulkCopy.addColumnMapping(i, metaEdge.getOutLabel() + Topology.OUT_VERTEX_COLUMN_END);
            this.columnMetadata.put(i++, new ColumnMetadata(
                    metaEdge.getOutLabel() + Topology.OUT_VERTEX_COLUMN_END,
                    sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(PropertyType.LONG)[0],
                    0,
                    0,
                    null,
                    PropertyType.LONG
            ));
        } else {
            for (String identifier : outVertexLabel.getIdentifiers()) {
                PropertyType propertyType = outVertexLabel.getProperty(identifier).orElseThrow(() -> new IllegalStateException(String.format("BUG: Did not find the identifier property %s.", identifier))).getPropertyType();
                bulkCopy.addColumnMapping(i, metaEdge.getOutLabel() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END);
                this.columnMetadata.put(i++, new ColumnMetadata(
                        metaEdge.getOutLabel() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END,
                        sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(propertyType)[0],
                        0,
                        0,
                        null,
                        propertyType
                ));
            }
        }
    }

    Object getValue(String column) {
        return this.currentRow.getRight().get(column);
    }

    @Override
    public Object[] getRowData() {
        List<Object> values = new ArrayList<>();
        addValues(values);
        if (this.inVertexLabel.hasIDPrimaryKey()) {
            values.add(((RecordId) this.currentRow.getMiddle().id()).sequenceId());
        } else {
            for (Object identifier : ((RecordId) this.currentRow.getMiddle().id()).getIdentifiers()) {
                values.add(identifier);
            }
        }
        if (this.outVertexLabel.hasIDPrimaryKey()) {
            values.add(((RecordId) this.currentRow.getLeft().id()).sequenceId());
        } else {
            for (Object identifier : ((RecordId) this.currentRow.getLeft().id()).getIdentifiers()) {
                values.add(identifier);
            }
        }
        return values.toArray();
    }

    @Override
    public boolean next() {
        if (this.rowIter.hasNext()) {
            Map.Entry<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> entry = this.rowIter.next();
            this.currentRow = entry.getValue();
            return true;
        } else {
            return false;
        }
    }
}
