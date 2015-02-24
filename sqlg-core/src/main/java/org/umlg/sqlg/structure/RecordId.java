package org.umlg.sqlg.structure;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;

import java.io.IOException;
import java.util.*;

/**
 * Date: 2015/02/21
 * Time: 8:50 PM
 */
public class RecordId implements KryoSerializable {

    public final static String RECORD_ID_DELIMITER = ":::";
    private SchemaTable schemaTable;
    private Long id;

    //For Kryo
    public RecordId() {
    }

    private RecordId(SchemaTable schemaTable, Long id) {
        this.schemaTable = schemaTable;
        this.id = id;
    }

    private RecordId(String label, Long id) {
        this.schemaTable = SqlgUtil.parseLabel(label);
        this.id = id;
    }

    public static RecordId from(SchemaTable schemaTable, Long id) {
        return new RecordId(schemaTable, id);
    }

    public static List<RecordId> from(Object... vertexId) {
        List<RecordId> result = new ArrayList<>(vertexId.length);
        for (Object o : vertexId) {
            if (o instanceof RecordId) {
                result.add((RecordId) o);
            } else {
                result.add(RecordId.from(o));
            }
        }
        return result;
    }

    public static RecordId from(Object vertexId) {
        if (!(vertexId instanceof String)) {
            throw SqlgExceptions.invalidId(vertexId.toString());
        }
        String stringId = (String) vertexId;
        String[] splittedId = stringId.split(RECORD_ID_DELIMITER);
        if (splittedId.length != 2) {
            throw SqlgExceptions.invalidId(vertexId.toString());
        }
        String label = splittedId[0];
        String id = splittedId[1];
        try {
            Long labelId = Long.valueOf(id);
            return new RecordId(label, labelId);
        } catch (NumberFormatException e) {
            throw SqlgExceptions.invalidId(vertexId.toString());
        }
    }

    public SchemaTable getSchemaTable() {
        return schemaTable;
    }

    public Long getId() {
        return id;
    }

    public static Map<SchemaTable, List<Long>> normalizeIds(List<RecordId> vertexId) {
        Map<SchemaTable, List<Long>> result = new HashMap<>();
        for (RecordId recordId : vertexId) {
            List<Long> ids = result.get(recordId.getSchemaTable());
            if (ids == null) {
                ids = new ArrayList<>();
                result.put(recordId.getSchemaTable(), ids);
            }
            ids.add(recordId.getId());
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(this.schemaTable.toString());
        result.append(RECORD_ID_DELIMITER);
        result.append(this.id.toString());
        return result.toString();
    }

    @Override
    public int hashCode() {
        return this.schemaTable.hashCode() + this.id.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (other == this) {
            return true;
        }
        if (!(other instanceof RecordId)) {
            return false;
        }
        RecordId otherRecordId = (RecordId) other;
        if (this.schemaTable.equals(otherRecordId.getSchemaTable())) {
            return this.id.equals(otherRecordId.getId());
        } else {
            return false;
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(this.getSchemaTable().getSchema().toString());
        output.writeString(this.getSchemaTable().getTable().toString());
        output.writeLong(this.getId());
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.schemaTable = schemaTable.of(input.readString(), input.readString());
        this.id = input.readLong();
    }

    static class RecordIdJacksonSerializer extends StdSerializer<RecordId> {
        public RecordIdJacksonSerializer() {
            super(RecordId.class);
        }

        @Override
        public void serialize(final RecordId customId, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(customId, jsonGenerator, false);
        }

        @Override
        public void serializeWithType(final RecordId customId, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(customId, jsonGenerator, true);
        }

        private void ser(final RecordId recordId, final JsonGenerator jsonGenerator, final boolean includeType) throws IOException {
            jsonGenerator.writeStartObject();

            if (includeType)
                jsonGenerator.writeStringField(GraphSONTokens.CLASS, RecordId.class.getName());

            SchemaTable schemaTable = recordId.getSchemaTable();
            jsonGenerator.writeObjectField("schema", schemaTable.getSchema());
            jsonGenerator.writeObjectField("table", schemaTable.getTable());
            jsonGenerator.writeObjectField("id", recordId.getId().toString());
            jsonGenerator.writeEndObject();
        }
    }

    static class CustomIdJacksonDeserializer extends StdDeserializer<RecordId> {
        public CustomIdJacksonDeserializer() {
            super(RecordId.class);
        }

        @Override
        public RecordId deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            String schema = null;
            String table = null;
            Long id = null;
            while (!jsonParser.getCurrentToken().isStructEnd()) {
                if (jsonParser.getText().equals("schema")) {
                    jsonParser.nextToken();
                    schema = jsonParser.getText();
                } else if (jsonParser.getText().equals("table")) {
                    jsonParser.nextToken();
                    table = jsonParser.getText();
                } else if (jsonParser.getText().equals("id")) {
                    jsonParser.nextToken();
                    id = Long.valueOf(jsonParser.getText());
                } else
                    jsonParser.nextToken();
            }

            if (!Optional.ofNullable(schema).isPresent())
                throw deserializationContext.mappingException("Could not deserialze RecordId: 'schema' is required");
            if (!Optional.ofNullable(table).isPresent())
                throw deserializationContext.mappingException("Could not deserialze RecordId: 'table' is required");
            if (!Optional.ofNullable(id).isPresent())
                throw deserializationContext.mappingException("Could not deserialze RecordId: 'id' is required");

            return new RecordId(SchemaTable.of(schema, table), id);
        }
    }
}
