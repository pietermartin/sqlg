package org.umlg.sqlg.sql.parse;

import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * List of column, managing serialization to SQL
 *
 * @author jpmoresmau
 */
public class ColumnList {
    /**
     * Column -> alias
     */
    private Map<Column, String> columns = new LinkedHashMap<>();

    /**
     * the graph to have access to the SQL dialect
     */
    private SqlgGraph sqlgGraph;


    /**
     * build a new empty column list
     *
     * @param graph
     */
    public ColumnList(SqlgGraph graph) {
        super();
        this.sqlgGraph = graph;
    }

    /**
     * add a new column
     *
     * @param schema
     * @param table
     * @param column
     * @param stepDepth
     * @param alias
     */
    public void add(String schema, String table, String column, int stepDepth, String alias) {
        Column c = new Column(schema, table, column, stepDepth);
        columns.put(c, alias);
    }

    /**
     * add a new column
     *
     * @param stt
     * @param column
     * @param alias
     */
    public void add(SchemaTableTree stt, String column, String alias) {
        add(stt.getSchemaTable(), column, stt.getStepDepth(), alias);
    }

    /**
     * add a new column
     *
     * @param st
     * @param column
     * @param stepDepth
     * @param alias
     */
    public void add(SchemaTable st, String column, int stepDepth, String alias) {
        add(st.getSchema(), st.getTable(), column, stepDepth, alias);
    }

    /**
     * get an alias if the column is already in the list
     *
     * @param schema
     * @param table
     * @param column
     * @return
     */
    public String getAlias(String schema, String table, String column, int stepDepth) {
        Column c = new Column(schema, table, column, stepDepth);
        return columns.get(c);
    }

    /**
     * get an alias if the column is already in the list
     *
     * @param stt
     * @param column
     * @return
     */
    public String getAlias(SchemaTableTree stt, String column) {
        return getAlias(stt.getSchemaTable(), column, stt.getStepDepth());
    }

    /**
     * get an alias if the column is already in the list
     *
     * @param st
     * @param column
     * @param stepDepth
     * @return
     */
    public String getAlias(SchemaTable st, String column, int stepDepth) {
        return getAlias(st.getSchema(), st.getTable(), column, stepDepth);
    }

    @Override
    public String toString() {
        String sep = "";
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Column, String> columnEntry : this.columns.entrySet()) {
            Column c = columnEntry.getKey();
            String alias = columnEntry.getValue();
            sb.append(sep);
            sep = ",\n\t";
            c.toString(sb);
            sb.append(" AS ");
            sb.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(alias));
        }
        return sb.toString();
    }

    /**
     * simple column, fully qualified: schema+table+column
     *
     * @author jpmoresmau
     */
    private class Column {
        private String schema;
        private String table;
        private String column;
        private int stepDepth = -1;

        public Column(String schema, String table, String column, int stepDepth) {
            super();
            this.schema = schema;
            this.table = table;
            this.column = column;
            this.stepDepth = stepDepth;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((column == null) ? 0 : column.hashCode());
            result = prime * result + ((schema == null) ? 0 : schema.hashCode());
            result = prime * result + ((table == null) ? 0 : table.hashCode());
            result = prime * result + stepDepth;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Column other = (Column) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (column == null) {
                if (other.column != null)
                    return false;
            } else if (!column.equals(other.column))
                return false;
            if (schema == null) {
                if (other.schema != null)
                    return false;
            } else if (!schema.equals(other.schema))
                return false;
            if (table == null) {
                if (other.table != null)
                    return false;
            } else if (!table.equals(other.table))
                return false;
            if (this.stepDepth != other.stepDepth)
                return false;
            return true;
        }

        private ColumnList getOuterType() {
            return ColumnList.this;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            toString(sb);
            return sb.toString();
        }

        /**
         * to string using provided builder
         *
         * @param sb
         */
        public void toString(StringBuilder sb) {
            sb.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
            sb.append(".");
            sb.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
            sb.append(".");
            sb.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column));
        }

    }
}
