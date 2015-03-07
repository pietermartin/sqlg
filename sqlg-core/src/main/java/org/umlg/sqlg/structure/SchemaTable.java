package org.umlg.sqlg.structure;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * Date: 2014/08/17
 * Time: 7:20 AM
 */

public class SchemaTable implements DataSerializable, Serializable {
    private String schema;
    private String table;

    //Needed for Hazelcast
    public SchemaTable() {
    }

    private SchemaTable(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public static SchemaTable of(String schema, String table) {
        return new SchemaTable(schema, table);
    }

    public static SchemaTable from(SqlgGraph sqlgGraph, final String label, String defaultSchema) {
        Objects.requireNonNull(label, "label may not be null!");
        String[] schemaLabel = label.split("\\.");
        final String schema;
        final String table;
        if (schemaLabel.length > 1) {
            schema = schemaLabel[0];
            table = label.substring(schema.length() + 1);
        } else {
            schema = defaultSchema;
            table = label;
        }
        sqlgGraph.getSqlDialect().validateSchemaName(schema);
        sqlgGraph.getSqlDialect().validateTableName(table);
        return SchemaTable.of(schema, table);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(this.schema);
        sb.append(".");
        sb.append(this.table);
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SchemaTable)) {
            return false;
        }
        if (o == this) {
            return true;
        }
        return this.toString().equals(o.toString());
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(this.schema);
        out.writeUTF(this.table);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.schema = in.readUTF();
        this.table = in.readUTF();
    }

    public boolean isVertexTable() {
        return this.table.startsWith(SchemaManager.VERTEX_PREFIX);
    }
}
