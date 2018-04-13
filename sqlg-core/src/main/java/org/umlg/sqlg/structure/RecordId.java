package org.umlg.sqlg.structure;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.io.graphson.AbstractObjectDeserializer;
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
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.KryoSerializable;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.umlg.sqlg.util.SqlgUtil;

import java.io.IOException;
import java.util.*;

/**
 * Date: 2015/02/21
 * Time: 8:50 PM
 */
public class RecordId implements KryoSerializable, Comparable {

    @SuppressWarnings("WeakerAccess")
    public final static String RECORD_ID_DELIMITER = ":::";
    private SchemaTable schemaTable;
    private Long id;
    private ListOrderedSet<Object> identifiers;

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

    private RecordId(SchemaTable schemaTable, ListOrderedSet<Object> identifiers) {
        this.schemaTable = schemaTable;
        this.identifiers = identifiers;
    }

    public static RecordId from(SchemaTable schemaTable, Long id) {
        return new RecordId(schemaTable, id);
    }

    public static RecordId from(SchemaTable schemaTable, ListOrderedSet<Object> identifiers) {
        return new RecordId(schemaTable, identifiers);
    }

    public static List<RecordId> from(Object... elementId) {
        List<RecordId> result = new ArrayList<>(elementId.length);
        for (Object o : elementId) {
            if (o instanceof RecordId) {
                result.add((RecordId) o);
            } else {
                result.add(RecordId.from(o));
            }
        }
        return result;
    }

    public static RecordId from(Object vertexId) {
        if (vertexId instanceof Element) {
            return (RecordId) ((SqlgElement)vertexId).id();
        }
        if (vertexId instanceof RecordId) {
            return (RecordId)vertexId;
        }
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

    public ListOrderedSet<Object> getIdentifiers() {
        return identifiers;
    }

    static Map<SchemaTable, List<Long>> normalizeIds(List<RecordId> vertexId) {
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
        return this.schemaTable.toString() +
                RECORD_ID_DELIMITER +
                (this.id != null ? this.id.toString() : this.identifiers.toString());
    }

    @Override
    public int hashCode() {
        int result = this.schemaTable.hashCode();
        if (this.id != null) {
            return result ^ this.id.hashCode();
        } else {
            StringBuilder sb = new StringBuilder();
            for (Object identifier : this.identifiers) {
                sb.append(identifier.toString());
            }
            return result ^ sb.toString().hashCode();
        }
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
        if (this.id != null && otherRecordId.id != null) {
            return this.schemaTable.equals(otherRecordId.getSchemaTable()) && this.id.equals(otherRecordId.getId());
        } else if (this.id == null && otherRecordId.id == null) {
            return this.schemaTable.equals(otherRecordId.getSchemaTable()) && this.identifiers.equals(otherRecordId.identifiers);
        } else {
            return false;
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(this.getSchemaTable().getSchema());
        output.writeString(this.getSchemaTable().getTable());
        output.writeLong(this.getId());
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.schemaTable = SchemaTable.of(input.readString(), input.readString());
        this.id = input.readLong();
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof RecordId)) {
            return -1;
        }
        RecordId other = (RecordId)o;
        int first = this.getSchemaTable().compareTo(other.getSchemaTable());
        if (first != 0) {
            return first;
        }
        return this.getId().compareTo(other.getId());
    }

    public boolean hasId() {
        return this.id != null;
    }

    @SuppressWarnings("DuplicateThrows")
    static class RecordIdJacksonSerializerV2d0 extends StdSerializer<RecordId> {
        RecordIdJacksonSerializerV2d0() {
            super(RecordId.class);
        }

        @Override
        public void serialize(final RecordId recordId, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            // when types are not embedded, stringify or resort to JSON primitive representations of the
            // type so that non-jvm languages can better interoperate with the TinkerPop stack.
            jsonGenerator.writeString(recordId.toString());
        }

        @Override
        public void serializeWithType(final RecordId recordId, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException, JsonProcessingException {
            // when the type is included add "class" as a key and then try to utilize as much of the
            // default serialization provided by jackson data-bind as possible.  for example, write
            // the uuid as an object so that when jackson serializes it, it uses the uuid serializer
            // to write it out with the type.  in this way, data-bind should be able to deserialize
            // it back when types are embedded.
            typeSerializer.writeTypePrefixForScalar(recordId, jsonGenerator);
            final Map<String, Object> m = new LinkedHashMap<>();
            m.put("schemaTable", recordId.getSchemaTable());
            m.put("id", recordId.getId());
            jsonGenerator.writeObject(m);
            typeSerializer.writeTypeSuffixForScalar(recordId, jsonGenerator);
        }
    }

    static class RecordIdJacksonDeserializerV2d0 extends AbstractObjectDeserializer<RecordId> {
        RecordIdJacksonDeserializerV2d0() {
            super(RecordId.class);
        }

        @Override
        public RecordId createObject(final Map data) {
            return RecordId.from((SchemaTable) data.get("schemaTable"), (Long) data.get("id"));
        }
    }

    static class RecordIdJacksonSerializerV3d0 extends StdScalarSerializer<RecordId> {
        public RecordIdJacksonSerializerV3d0() {
            super(RecordId.class);
        }

        @Override
        public void serialize(final RecordId recordId, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            final Map<String, Object> m = new HashMap<>();
            m.put("schemaTable", recordId.getSchemaTable());
            m.put("id", recordId.getId());
            jsonGenerator.writeObject(m);
        }

    }

    static class RecordIdJacksonDeserializerV3d0 extends StdDeserializer<RecordId> {
        public RecordIdJacksonDeserializerV3d0() {
            super(RecordId.class);
        }

        @Override
        public RecordId deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final Map<String, Object> data = deserializationContext.readValue(jsonParser, Map.class);
            return RecordId.from((SchemaTable) data.get("schemaTable"), (Long) data.get("id"));
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

}
