package org.umlg.sqlg.structure.topology;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.umlg.sqlg.structure.SchemaTable;

/**
 * This class is a data holder for the foreign keys of an edge.
 * If the in or out vertex label does not have user defined primary keys then there will be only one entry to
 * the vertex's 'ID'. If the vertex label has user defined primary keys there there will be many. One for each of the
 * composite keys.
 *
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/03/29
 */
public class ForeignKey {

    private final ListOrderedSet<String> compositeKeys = new ListOrderedSet<>();
    private String concatenatedIdentifiers = "";
    private Direction direction;
    private SchemaTable schemaTable;

    public static ForeignKey of(String key) {
        return new ForeignKey(key);
    }

    ForeignKey() {
    }

    private ForeignKey(String key) {
        this.compositeKeys.add(key);
        this.concatenatedIdentifiers += key;
        this.direction = key.endsWith(Topology.IN_VERTEX_COLUMN_END) ? Direction.IN : Direction.OUT;
        int indexOfDot = key.indexOf(".");
        String foreignKeySchema = key.substring(0, indexOfDot);
        String foreignKeyTable = key.substring(indexOfDot + 1);
        this.schemaTable = SchemaTable.of(foreignKeySchema, foreignKeyTable);
    }

    public void add(String label, String identifier, String suffix) {
        this.compositeKeys.add(label + "." + identifier + suffix);
        this.concatenatedIdentifiers += label + "." + identifier + suffix;
        Direction dir = suffix.equals(Topology.IN_VERTEX_COLUMN_END) ? Direction.IN : Direction.OUT;
        int indexOfDot = label.indexOf(".");
        String foreignKeySchema = label.substring(0, indexOfDot);
        String foreignKeyTable = label.substring(indexOfDot + 1);
        this.schemaTable = SchemaTable.of(foreignKeySchema, foreignKeyTable);
        if (this.direction == null) {
            //Called for the first add.
            this.direction = dir;
            this.schemaTable = SchemaTable.of(foreignKeySchema, foreignKeyTable);
        } else {
            Preconditions.checkState(this.direction == dir);
            Preconditions.checkState(this.schemaTable.getSchema().equals(foreignKeySchema));
            Preconditions.checkState(this.schemaTable.getTable().equals(foreignKeyTable));
        }
    }

    @Override
    public int hashCode() {
        return this.concatenatedIdentifiers.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ForeignKey)) {
            return false;
        }
        ForeignKey other = (ForeignKey) o;
        return this.concatenatedIdentifiers.equals(other.concatenatedIdentifiers);
    }

    public boolean isFor(SchemaTable schemaTable) {
        return this.schemaTable.equals(schemaTable);
    }

    public Direction getDirection() {
        return direction;
    }

    public SchemaTable getSchemaTable() {
        return schemaTable;
    }

    public boolean isIn() {
        return this.direction == Direction.IN;
    }

    public boolean isOut() {
        return this.direction == Direction.OUT;
    }

    public ListOrderedSet<String> getCompositeKeys() {
        return this.compositeKeys;
    }
}
