package org.umlg.sqlg.structure;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.io.Serializable;

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

    public SchemaTable(String schema, String table) {
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
}
