package org.umlg.sqlg.structure;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Preconditions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;

import javax.xml.validation.Schema;
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
        return this.schema + "." + this.table;
    }

    @Override
    public int hashCode() {
        return (this.schema + this.table).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SchemaTable)) {
            return false;
        }
        if (o == this) {
            return true;
        }
        SchemaTable other = (SchemaTable) o;
        return this.schema.equals(other.schema) && this.table.equals(other.table);
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

    public boolean isEdgeTable() {
        return !isVertexTable();
    }

    public SchemaTable withOutPrefix() {
        Preconditions.checkState(this.table.startsWith(SchemaManager.VERTEX_PREFIX) || this.table.startsWith(SchemaManager.EDGE_PREFIX));
        if (this.table.startsWith(SchemaManager.VERTEX_PREFIX))
            return SchemaTable.of(this.getSchema(), this.getTable().substring(SchemaManager.VERTEX_PREFIX.length()));
        else
            return SchemaTable.of(this.getSchema(), this.getTable().substring(SchemaManager.EDGE_PREFIX.length()));
    }

    static class SchemaTableJacksonSerializer extends StdSerializer<SchemaTable> {
        public SchemaTableJacksonSerializer() {
            super(SchemaTable.class);
        }

        @Override
        public void serialize(final SchemaTable schemaTable, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(schemaTable, jsonGenerator, false);
        }

        @Override
        public void serializeWithType(final SchemaTable schemaTable, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(schemaTable, jsonGenerator, true);
        }

        private void ser(final SchemaTable schemaTable, final JsonGenerator jsonGenerator, final boolean includeType) throws IOException {
            if (includeType) {
                // when the type is included add "class" as a key and then try to utilize as much of the
                // default serialization provided by jackson data-bind as possible.  for example, write
                // the uuid as an object so that when jackson serializes it, it uses the uuid serializer
                // to write it out with the type.  in this way, data-bind should be able to deserialize
                // it back when types are embedded.
                jsonGenerator.writeStartObject();
                jsonGenerator.writeStringField(GraphSONTokens.CLASS, SchemaTable.class.getName());
                jsonGenerator.writeStringField("schema", schemaTable.getSchema());
                jsonGenerator.writeObjectField("table", schemaTable.getTable());
                jsonGenerator.writeEndObject();
            } else {
                // when types are not embedded, stringify or resort to JSON primitive representations of the
                // type so that non-jvm languages can better interoperate with the TinkerPop stack.
                jsonGenerator.writeString(schemaTable.toString());
            }
        }
    }
}
