package org.umlg.sqlg.sql.parse;

import org.apache.commons.lang3.tuple.Pair;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.Topology;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * List of column, managing serialization to SQL
 *
 * @author jpmoresmau
 */
public class ColumnList {
    /**
     * Column -> alias
     */
    private LinkedHashMap<Column, String> columns = new LinkedHashMap<>();
    /**
     * Alias -> Column
     */
    private LinkedHashMap<String, Column> aliases = new LinkedHashMap<>();

    /**
     * Indicates that the query is for a {@link org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep}
     * In this case only the first column will be returned.
     */
    private boolean drop;

    /**
     * the graph to have access to the SQL dialect
     */
    private SqlgGraph sqlgGraph;

    /**
     * A map of all the properties and their types.
     */
    private Map<String, Map<String, PropertyType>> filteredAllTables;

    /**
     * build a new empty column list
     *
     * @param graph
     * @param drop
     * @param filteredAllTables
     */
    public ColumnList(SqlgGraph graph, boolean drop, Map<String, Map<String, PropertyType>> filteredAllTables) {
        super();
        this.sqlgGraph = graph;
        this.drop = drop;
        this.filteredAllTables = filteredAllTables;
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
    private void add(String schema, String table, String column, int stepDepth, String alias) {
        Column c = new Column(schema, table, column, this.filteredAllTables.get(schema + "." + table).get(column), stepDepth);
        this.columns.put(c, alias);
        this.aliases.put(alias, c);
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
    private String getAlias(String schema, String table, String column, int stepDepth) {
        //PropertyType is not part of equals or hashCode so not needed for the lookup.
        Column c = new Column(schema, table, column, null, stepDepth);
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
            if (this.drop) {
                break;
            }
        }
        return sb.toString();
    }

    public Column getColumn(String alias) {
        return this.aliases.get(alias);
    }

    public Pair<String, PropertyType> getPropertyType(String alias) {
        Column column = this.aliases.get(alias);
        if (column != null) {
            return Pair.of(column.column, column.propertyType);
        } else {
            return null;
        }
    }

    public String toString(String prefix) {
        StringBuilder sb = new StringBuilder();
        int i = 1;
        List<String> fromAliases = this.aliases.keySet().stream().filter(
                (alias) -> !alias.endsWith(Topology.IN_VERTEX_COLUMN_END) && !alias.endsWith(Topology.OUT_VERTEX_COLUMN_END))
                .collect(Collectors.toList());
        for (String alias : fromAliases) {
            sb.append(prefix);
            sb.append(".");
            sb.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(alias));
            if (i++ < fromAliases.size()) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    public LinkedHashMap<Column, String> getFor(int stepDepth, SchemaTable schemaTable) {
        LinkedHashMap<Column, String> result = new LinkedHashMap<>();
        for (Column column : this.columns.keySet()) {
            if (column.isFor(stepDepth, schemaTable)) {
                result.put(column, this.columns.get(column));
            }
        }
        return result;
    }

    public int indexColumns(int startColumnIndex) {
        int i = startColumnIndex;
        for (Column column : columns.keySet()) {
            column.columnIndex = i++;
        }
        return i++;
    }

    public int indexColumnsExcludeForeignKey(int startColumnIndex) {
        int i = startColumnIndex;
        for (String alias : this.aliases.keySet()) {
            if (!alias.endsWith(Topology.IN_VERTEX_COLUMN_END) && !alias.endsWith(Topology.OUT_VERTEX_COLUMN_END)) {
                this.aliases.get(alias).columnIndex = i++;
            }
        }
        return i++;
    }

    /**
     * simple column, fully qualified: schema+table+column
     *
     * @author jpmoresmau
     */
    public class Column {
        private String schema;
        private String table;
        private String column;
        private int stepDepth = -1;
        private PropertyType propertyType;
        private boolean ID;
        private int columnIndex = -1;

        public Column(String schema, String table, String column, PropertyType propertyType, int stepDepth) {
            super();
            this.schema = schema;
            this.table = table;
            this.column = column;
            this.propertyType = propertyType;
            this.stepDepth = stepDepth;
            this.ID = this.column.equals(Topology.ID);
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

        public String getSchema() {
            return schema;
        }

        public String getTable() {
            return table;
        }

        public String getColumn() {
            return column;
        }

        public int getStepDepth() {
            return stepDepth;
        }

        public PropertyType getPropertyType() {
            return propertyType;
        }

        public boolean isID() {
            return ID;
        }

        public int getColumnIndex() {
            return columnIndex;
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

        public boolean isFor(int stepDepth, SchemaTable schemaTable) {
            return this.stepDepth == stepDepth && this.schema.equals(schemaTable.getSchema()) && this.table.equals(schemaTable.getTable());
        }

    }
}
