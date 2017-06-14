package org.umlg.sqlg.structure;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.io.graphson.AbstractObjectDeserializer;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerationException;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Date: 2014/08/17
 * Time: 7:20 AM
 */

public class SchemaTable implements Serializable, Comparable {
    private String schema;
    private String table;

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

    public static SchemaTable from(SqlgGraph sqlgGraph, final String label) {
        Objects.requireNonNull(label, "label may not be null!");
        int indexOfPeriod = label.indexOf(".");
        final String schema;
        final String table;
        if (indexOfPeriod == -1) {
            schema = sqlgGraph.getSqlDialect().getPublicSchema();
            table = label;
        } else {
            schema = label.substring(0, indexOfPeriod);
            table = label.substring(indexOfPeriod + 1);
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
        int result = this.schema.hashCode();
        return result ^ this.table.hashCode();
//        return (this.schema + this.table).hashCode();
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

    public SchemaTable withPrefix(String prefix) {
        Preconditions.checkArgument(prefix.equals(SchemaManager.VERTEX_PREFIX) || prefix.equals(SchemaManager.EDGE_PREFIX), "Prefix must be either " + SchemaManager.VERTEX_PREFIX + " or " + SchemaManager.EDGE_PREFIX + " for " + prefix);
        Preconditions.checkState(!this.table.startsWith(SchemaManager.VERTEX_PREFIX) && !this.table.startsWith(SchemaManager.EDGE_PREFIX), "SchemaTable is already prefixed.");
        return SchemaTable.of(this.getSchema(), prefix + this.getTable());
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof SchemaTable)) {
            return -1;
        }
        return toString().compareTo(o.toString());
    }

    @SuppressWarnings("DuplicateThrows")
    static class SchemaTableIdJacksonSerializerV2d0 extends StdSerializer<SchemaTable> {
        SchemaTableIdJacksonSerializerV2d0() {
            super(SchemaTable.class);
        }

        @Override
        public void serialize(final SchemaTable schemaTable, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            // when types are not embedded, stringify or resort to JSON primitive representations of the
            // type so that non-jvm languages can better interoperate with the TinkerPop stack.
            jsonGenerator.writeString(schemaTable.toString());
        }

        @Override
        public void serializeWithType(final SchemaTable schemaTable, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException, JsonProcessingException {
            // when the type is included add "class" as a key and then try to utilize as much of the
            // default serialization provided by jackson data-bind as possible.  for example, write
            // the uuid as an object so that when jackson serializes it, it uses the uuid serializer
            // to write it out with the type.  in this way, data-bind should be able to deserialize
            // it back when types are embedded.
            typeSerializer.writeTypePrefixForScalar(schemaTable, jsonGenerator);
            final Map<String, Object> m = new LinkedHashMap<>();
            m.put("schema", schemaTable.getSchema());
            m.put("table", schemaTable.getTable());
            jsonGenerator.writeObject(m);
            typeSerializer.writeTypeSuffixForScalar(schemaTable, jsonGenerator);
        }
    }

    static class SchemaTableIdJacksonDeserializerV2d0 extends AbstractObjectDeserializer<SchemaTable> {
        SchemaTableIdJacksonDeserializerV2d0() {
            super(SchemaTable.class);
        }

        @Override
        public SchemaTable createObject(final Map data) {
            return SchemaTable.of((String)data.get("schema"), (String) data.get("table"));
        }
    }
}
