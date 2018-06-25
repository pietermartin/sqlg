package org.umlg.sqlg.structure;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.io.graphson.AbstractObjectDeserializer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerationException;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdScalarSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.umlg.sqlg.structure.topology.Topology;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.umlg.sqlg.structure.topology.Topology.EDGE_PREFIX;
import static org.umlg.sqlg.structure.topology.Topology.VERTEX_PREFIX;

/**
 * Date: 2014/08/17
 * Time: 7:20 AM
 */

public class SchemaTable implements Serializable, Comparable {
    private String schema;
    private String table;
    /**
     * Indicates that this represents a temporary table.
     */
    private boolean temporary;

    private SchemaTable(String schema, String table, boolean temporary) {
        this.schema = schema;
        this.table = table;
        this.temporary = temporary;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public static SchemaTable of(String schema, String table) {
        return of(schema, table, false);
    }

    public static SchemaTable of(String schema, String table, boolean temporary) {
        return new SchemaTable(schema, table, temporary);
    }

    public static SchemaTable from(SqlgGraph sqlgGraph, final String label) {
        return from(sqlgGraph, label, false);
    }

    public static SchemaTable from(SqlgGraph sqlgGraph, final String label, boolean temporary) {
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
        return SchemaTable.of(schema, table, temporary);
    }

    @Override
    public String toString() {
        return this.schema + "." + this.table;
    }

    @Override
    public int hashCode() {
        int result = this.schema.hashCode();
        return result ^ this.table.hashCode();
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
        return this.table.startsWith(VERTEX_PREFIX);
    }

    public boolean isEdgeTable() {
        return !isVertexTable();
    }

    public boolean isTemporary() {
        return this.temporary;
    }

    public SchemaTable withOutPrefix() {
        Preconditions.checkState(this.table.startsWith(VERTEX_PREFIX) || this.table.startsWith(EDGE_PREFIX));
        if (this.table.startsWith(VERTEX_PREFIX))
            return SchemaTable.of(this.getSchema(), this.getTable().substring(VERTEX_PREFIX.length()));
        else
            return SchemaTable.of(this.getSchema(), this.getTable().substring(EDGE_PREFIX.length()));
    }

    public SchemaTable withPrefix(String prefix) {
        Preconditions.checkArgument(prefix.equals(VERTEX_PREFIX) || prefix.equals(EDGE_PREFIX), "Prefix must be either " + VERTEX_PREFIX + " or " + EDGE_PREFIX + " for " + prefix);
        Preconditions.checkState(!this.table.startsWith(VERTEX_PREFIX) && !this.table.startsWith(EDGE_PREFIX), "SchemaTable is already prefixed.");
        return SchemaTable.of(this.getSchema(), prefix + this.getTable());
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof SchemaTable)) {
            return -1;
        }
        return toString().compareTo(o.toString());
    }

    public boolean isWithPrefix() {
        return this.table.startsWith(Topology.VERTEX_PREFIX) || this.table.startsWith(Topology.EDGE_PREFIX);
    }

    static class SchemaTableIdJacksonSerializerV1d0 extends StdSerializer<SchemaTable> {
        SchemaTableIdJacksonSerializerV1d0() {
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
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(GraphSONTokens.CLASS, SchemaTable.class.getName());
            jsonGenerator.writeStringField("schema", schemaTable.getSchema());
            jsonGenerator.writeStringField("table", schemaTable.getTable());
            jsonGenerator.writeEndObject();
        }
    }

    static class SchemaTableIdJacksonDeserializerV1d0 extends StdDeserializer<SchemaTable> {
        SchemaTableIdJacksonDeserializerV1d0() {
            super(SchemaTable.class);
        }

        @Override
        public SchemaTable deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            org.apache.tinkerpop.shaded.jackson.core.JsonToken jsonToken = jsonParser.nextToken();
            Preconditions.checkState(org.apache.tinkerpop.shaded.jackson.core.JsonToken.VALUE_STRING == jsonToken);
            String schema = deserializationContext.readValue(jsonParser, String.class);
            jsonToken = jsonParser.nextToken();
            Preconditions.checkState(org.apache.tinkerpop.shaded.jackson.core.JsonToken.FIELD_NAME == jsonToken);
            jsonToken = jsonParser.nextToken();
            Preconditions.checkState(org.apache.tinkerpop.shaded.jackson.core.JsonToken.VALUE_STRING == jsonToken);
            String table = deserializationContext.readValue(jsonParser, String.class);
            jsonToken = jsonParser.nextToken();
            Preconditions.checkState(org.apache.tinkerpop.shaded.jackson.core.JsonToken.END_OBJECT == jsonToken);
            return SchemaTable.of(schema, table);
        }

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

    @SuppressWarnings("DuplicateThrows")
    static class SchemaTableJacksonSerializerV3d0 extends StdScalarSerializer<SchemaTable> {
        SchemaTableJacksonSerializerV3d0() {
            super(SchemaTable.class);
        }

        @Override
        public void serialize(final SchemaTable schemaTable, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            // when types are not embedded, stringify or resort to JSON primitive representations of the
            // type so that non-jvm languages can better interoperate with the TinkerPop stack.
            final Map<String, Object> m = new LinkedHashMap<>();
            m.put("schema", schemaTable.getSchema());
            m.put("table", schemaTable.getTable());
            jsonGenerator.writeObject(m);
        }

    }

    static class SchemaTableJacksonDeserializerV3d0 extends StdDeserializer<SchemaTable> {
        public SchemaTableJacksonDeserializerV3d0() {
            super(SchemaTable.class);
        }

        @Override
        public SchemaTable deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final Map<String, Object> data = deserializationContext.readValue(jsonParser, Map.class);
            return SchemaTable.of((String)data.get("schema"), (String) data.get("table"));
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

}
