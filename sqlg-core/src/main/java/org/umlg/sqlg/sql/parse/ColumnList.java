package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.Topology;

import java.util.*;
import java.util.stream.Collectors;

/**
 * List of column, managing serialization to SQL
 *
 * @author jpmoresmau
 * @author pieter
 */
public class ColumnList {
    /**
     * Column -> alias
     */
    private final LinkedHashMap<Column, String> columns = new LinkedHashMap<>();
    /**
     * Alias -> Column
     */
    private final LinkedHashMap<String, Column> aliases = new LinkedHashMap<>();

    /**
     * Indicates that the query is for a {@link org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep}
     * In this case only the first column will be returned.
     */
    private final boolean drop;

    /**
     * the graph to have access to the SQL dialect
     */
    private final SqlgGraph sqlgGraph;

    /**
     * A map of all the properties and their types.
     */
    private final Map<String, Map<String, PropertyType>> filteredAllTables;

    private final ListOrderedSet<String> identifiers;

    /**
     * Indicates if any of the Column's have an aggregateFunction
     */
    private boolean containsAggregate;

    /**
     * build a new empty column list
     *
     * @param graph
     * @param drop
     * @param filteredAllTables
     */
    public ColumnList(SqlgGraph graph, boolean drop, ListOrderedSet<String> identifiers, Map<String, Map<String, PropertyType>> filteredAllTables) {
        super();
        this.sqlgGraph = graph;
        this.drop = drop;
        this.filteredAllTables = filteredAllTables;
        this.identifiers = identifiers;
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
    private Column internalAdd(String schema, String table, String column, int stepDepth, String alias, String aggregateFunction) {
        Column c = new Column(schema, table, column, this.filteredAllTables.get(schema + "." + table).get(column), stepDepth, aggregateFunction);
        this.columns.put(c, alias);
        this.aliases.put(alias, c);
        this.containsAggregate = this.containsAggregate || aggregateFunction != null;
        return c;
    }

    public Column add(SchemaTable st, String column, int stepDepth, String alias, String aggregateFunction) {
        return internalAdd(st.getSchema(), st.getTable(), column, stepDepth, alias, aggregateFunction);
    }

    /**
     * add a new column
     *
     * @param st
     * @param column
     * @param stepDepth
     * @param alias
     */
    public Column add(SchemaTable st, String column, int stepDepth, String alias) {
        return internalAdd(st.getSchema(), st.getTable(), column, stepDepth, alias, null);
    }

    /**
     * add a new column
     *
     * @param schema          The column's schema
     * @param table           The column's table
     * @param column          The column
     * @param stepDepth       The column's step depth.
     * @param alias           The column's alias.
     * @param foreignKeyParts The foreign key column broken up into its parts. schema, table and for user supplied identifiers the property name.
     */
    private void addForeignKey(String schema, String table, String column, int stepDepth, String alias, String[] foreignKeyParts) {
        Column c = internalAdd(schema, table, column, stepDepth, alias, null);
        c.isForeignKey = true;
        if (foreignKeyParts.length == 3) {
            Map<String, PropertyType> properties = this.filteredAllTables.get(foreignKeyParts[0] + "." + Topology.VERTEX_PREFIX + foreignKeyParts[1]);
            if (foreignKeyParts[2].endsWith(Topology.IN_VERTEX_COLUMN_END)) {
                c.propertyType = properties.get(foreignKeyParts[2].substring(0, foreignKeyParts[2].length() - Topology.IN_VERTEX_COLUMN_END.length()));
                c.foreignKeyDirection = Direction.IN;
                c.foreignSchemaTable = SchemaTable.of(foreignKeyParts[0], foreignKeyParts[1]);
                c.foreignKeyProperty = foreignKeyParts[2];
            } else {
                c.propertyType = properties.get(foreignKeyParts[2].substring(0, foreignKeyParts[2].length() - Topology.OUT_VERTEX_COLUMN_END.length()));
                c.foreignKeyDirection = Direction.OUT;
                c.foreignSchemaTable = SchemaTable.of(foreignKeyParts[0], foreignKeyParts[1]);
                c.foreignKeyProperty = foreignKeyParts[2];
            }
        } else {
            c.propertyType = PropertyType.LONG;
            c.foreignKeyDirection = (column.endsWith(Topology.IN_VERTEX_COLUMN_END) ? Direction.IN : Direction.OUT);
            c.foreignSchemaTable = SchemaTable.of(foreignKeyParts[0], foreignKeyParts[1].substring(0, foreignKeyParts[1].length() - Topology.IN_VERTEX_COLUMN_END.length()));
            c.foreignKeyProperty = null;
        }
    }


    void addForeignKey(SchemaTableTree stt, String column, String alias) {
        String[] foreignKeyParts = column.split("\\.");
        Preconditions.checkState(foreignKeyParts.length == 2 || foreignKeyParts.length == 3, "Edge table foreign must be schema.table__I\\O or schema.table.property__I\\O. Found %s", column);
        addForeignKey(stt.getSchemaTable().getSchema(), stt.getSchemaTable().getTable(), column, stt.getStepDepth(), alias, foreignKeyParts);
    }

    /**
     * get an alias if the column is already in the list
     *
     * @param schema
     * @param table
     * @param column
     * @return
     */
    private String getAlias(String schema, String table, String column, int stepDepth, String aggregateFunction) {
        //PropertyType is not part of equals or hashCode so not needed for the lookup.
        Column c = new Column(schema, table, column, null, stepDepth, aggregateFunction);
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
        return getAlias(stt.getSchemaTable(), column, stt.getStepDepth(), stt.getAggregateFunction() == null ? null : stt.getAggregateFunction().getLeft());
    }

    /**
     * get an alias if the column is already in the list
     *
     * @param st
     * @param column
     * @param stepDepth
     * @return
     */
    public String getAlias(SchemaTable st, String column, int stepDepth, String aggregateFunction) {
        return getAlias(st.getSchema(), st.getTable(), column, stepDepth, aggregateFunction);
    }

    public String toSelectString(boolean partOfDuplicateQuery) {
        StringBuilder sb = new StringBuilder();
        int countIdentifiers = 1;
        boolean first = true;
        for (Map.Entry<Column, String> columnEntry : this.columns.entrySet()) {
            Column c = columnEntry.getKey();
            String alias = columnEntry.getValue();
            String part = c.toSelectString(partOfDuplicateQuery, alias);
            if (part != null) {
                if (!first) {
                    sb.append(",\n\t");
                }
                sb.append(part);
            }
            first = false;
            if (this.drop && (this.identifiers.isEmpty() || countIdentifiers++ == this.identifiers.size())) {
                break;
            }
        }
        return sb.toString();
    }

    public boolean isContainsAggregate() {
        return containsAggregate;
    }

    @Override
    public String toString() {
        return toSelectString(false);
    }

    public Pair<String, PropertyType> getPropertyType(String alias) {
        Column column = this.aliases.get(alias);
        if (column != null) {
            return Pair.of(column.column, column.propertyType);
        } else {
            return null;
        }
    }

    public void removeColumns(SchemaTableTree schemaTableTree) {
        Set<Column> toRemove = new HashSet<>();
        for (Column column : this.columns.keySet()) {
            if (column.aggregateFunction == null && !(schemaTableTree.getGroupBy() != null && schemaTableTree.getGroupBy().contains(column.column))) {
                toRemove.add(column);
            }
        }
        for (Column column : toRemove) {
            this.columns.remove(column);
        }
        Set<String> toRemoveAliases = new HashSet<>();
        for (String alias : this.aliases.keySet()) {
            Column column = this.aliases.get(alias);
            if (toRemove.contains(column)) {
                toRemoveAliases.add(alias);
            }
        }
        for (String toRemoveAlias : toRemoveAliases) {
            this.aliases.remove(toRemoveAlias);
        }
    }

    public String toOuterFromString(String prefix, boolean stackContainsAggregate) {
        StringBuilder sb = new StringBuilder();
        List<String> fromAliases = this.aliases.keySet().stream()
                .filter((alias) -> !alias.endsWith(Topology.IN_VERTEX_COLUMN_END) && !alias.endsWith(Topology.OUT_VERTEX_COLUMN_END))
                .collect(Collectors.toList());
        for (String alias : fromAliases) {
            Column c = this.aliases.get(alias);
            if (stackContainsAggregate && (c.isID() || c.isForeignKey())) {
                continue;
            }
            if (c.aggregateFunction != null) {
                sb.append(c.aggregateFunction.toUpperCase());
                sb.append("(");
            }
            if (c.aggregateFunction != null && c.aggregateFunction.equals(GraphTraversal.Symbols.count)) {
                sb.append("1)");
            } else {
                sb.append(prefix);
                sb.append(".");
                sb.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(alias));
                if (c.aggregateFunction != null && c.aggregateFunction.equalsIgnoreCase("avg")) {
                    sb.append("), COUNT(1) AS ").append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(alias + "_weight"));
                } else if (c.aggregateFunction != null) {
                    sb.append(")");
                }
            }
            sb.append(", ");
        }
        if (sb.length() > 1) {
            sb.deleteCharAt(sb.length() - 2);
        }
        return sb.toString();
    }

    public Map<SchemaTable, List<Column>> getInForeignKeys(int stepDepth, SchemaTable schemaTable) {
        return getForeignKeys(stepDepth, schemaTable, Direction.IN);
    }

    public Map<SchemaTable, List<Column>> getOutForeignKeys(int stepDepth, SchemaTable schemaTable) {
        return getForeignKeys(stepDepth, schemaTable, Direction.OUT);
    }

    private Map<SchemaTable, List<Column>> getForeignKeys(int stepDepth, SchemaTable schemaTable, Direction direction) {
        Map<SchemaTable, List<Column>> result = new HashMap<>();
        for (Column column : this.columns.keySet()) {
            if (column.isForeignKey && column.foreignKeyDirection == direction && column.isFor(stepDepth, schemaTable)) {
                List<Column> columns = result.computeIfAbsent(column.getForeignSchemaTable(), (k) -> new ArrayList<>());
                columns.add(column);
            }
        }
        return result;
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

    public void indexColumns(int startColumnIndex) {
        int i = startColumnIndex;
        for (Column column : columns.keySet()) {
            column.columnIndex = i++;
            if (column.aggregateFunction != null && column.aggregateFunction.equals("avg")) {
                column.columnIndex = i++;
            }
//            if (!this.containsAggregate) {
//                column.columnIndex = i++;
//            } else if (!column.isID() && !column.isForeignKey) {
//                column.columnIndex = i++;
//            }
        }
    }

    public int reindexColumnsExcludeForeignKey(int startColumnIndex, boolean stackContainsAggregate) {
        int i = startColumnIndex;
        for (String alias : this.aliases.keySet()) {
            Column column = this.aliases.get(alias);
            if (stackContainsAggregate && (
                    column.isID() ||
                    column.isForeignKey()) ||
                    alias.endsWith(Topology.IN_VERTEX_COLUMN_END) ||
                    alias.endsWith(Topology.OUT_VERTEX_COLUMN_END)) {

                continue;
            }
            column.columnIndex = i++;
            if (stackContainsAggregate && column.aggregateFunction != null && column.aggregateFunction.equalsIgnoreCase("avg")) {
                column.columnIndex = i++;
            }
        }
        return i;
    }

    /**
     * simple column, fully qualified: schema+table+column
     *
     * @author jpmoresmau
     */
    public class Column {
        private final String schema;
        private final String table;
        private final String column;
        private final int stepDepth;
        private final boolean ID;
        private final String aggregateFunction;
        private PropertyType propertyType;
        private int columnIndex = -1;
        //Foreign key properties
        private boolean isForeignKey;
        private Direction foreignKeyDirection;
        private SchemaTable foreignSchemaTable;
        //Only set for user identifier primary keys
        private String foreignKeyProperty;

        Column(String schema, String table, String column, PropertyType propertyType, int stepDepth, String aggregateFunction) {
            super();
            this.schema = schema;
            this.table = table;
            this.column = column;
            this.isForeignKey = column.endsWith(Topology.IN_VERTEX_COLUMN_END) || column.endsWith(Topology.OUT_VERTEX_COLUMN_END);
            this.propertyType = propertyType;
            this.stepDepth = stepDepth;
            this.ID = this.column.equals(Topology.ID);
            this.aggregateFunction = aggregateFunction;
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

            if (this.aggregateFunction == null) {
                if (other.aggregateFunction != null) {
                    return false;
                }
            } else if (!this.aggregateFunction.equals(other.aggregateFunction)) {
                return false;
            }
            return this.stepDepth == other.stepDepth;
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

        public boolean isForeignKey() {
            return isForeignKey;
        }

        public Direction getForeignKeyDirection() {
            return foreignKeyDirection;
        }

        public SchemaTable getForeignSchemaTable() {
            return foreignSchemaTable;
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        boolean isForeignKeyProperty() {
            return foreignKeyProperty != null;
        }

        @Override
        public String toString() {
            return toSelectString(false, "");
        }

        /**
         * to string using provided builder
         */
        String toSelectString(boolean partOfDuplicateQuery, String alias) {
            if (partOfDuplicateQuery && this.aggregateFunction != null && this.aggregateFunction.equals(GraphTraversal.Symbols.count)) {
                return null;
            } else {
                return ColumnList.this.sqlgGraph.getSqlDialect().toSelectString(partOfDuplicateQuery, this, alias);
            }
        }

        boolean isFor(int stepDepth, SchemaTable schemaTable) {
            return this.stepDepth == stepDepth &&
                    this.schema != null && this.schema.equals(schemaTable.getSchema()) &&
                    this.table != null && this.table.equals(schemaTable.getTable());
        }

        public boolean isForeignKey(int stepDepth, SchemaTable schemaTable) {
            return this.stepDepth == stepDepth && this.schema.equals(schemaTable.getSchema()) && this.table.equals(schemaTable.getTable());
        }

        public String getAggregateFunction() {
            return aggregateFunction;
        }
    }
}
