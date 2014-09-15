package org.umlg.sqlg.structure;

/**
 * Date: 2014/08/17
 * Time: 7:20 AM
 */

public class SchemaTable {
    private String schema;
    private String table;

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
}
