package org.umlg.sqlg.structure;

/**
 * Date: 2016/08/09
 * Time: 11:32 AM
 */
public class MetaEdge {

    private final SchemaTable schemaTable;
    private final String outLabel;
    private final String inLabel;

    private MetaEdge(SchemaTable schemaTable, String outLabel, String inLabel) {
        this.schemaTable = schemaTable;
        this.outLabel = outLabel;
        this.inLabel = inLabel;
    }

    public static MetaEdge from(SchemaTable outSchemaTable, SqlgVertex outVertex, SqlgVertex inVertex) {
        return new MetaEdge(outSchemaTable, outVertex.schema + "." + outVertex.table, inVertex.schema + "." + inVertex.table);
    }

    public SchemaTable getSchemaTable() {
        return schemaTable;
    }

    public String getOutLabel() {
        return outLabel;
    }

    public String getInLabel() {
        return inLabel;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MetaEdge)) {
            return false;
        }
        MetaEdge other = (MetaEdge)o;
        return other.schemaTable.equals(this.schemaTable) && other.outLabel.equals(this.outLabel) && other.inLabel.equals(this.inLabel);
    }

    @Override
    public int hashCode() {
        return (this.schemaTable.getSchema() + this.schemaTable.getTable() + this.outLabel + this.inLabel).hashCode();
    }
}
