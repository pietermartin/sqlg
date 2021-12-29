package org.umlg.sqlg.structure;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.io.graphson.AbstractObjectDeserializer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.shaded.jackson.core.*;
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
import org.umlg.sqlg.structure.topology.AbstractLabel;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.VertexLabel;
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
    private ID id;

    //For Kryo
    public RecordId() {
    }

    private RecordId(SchemaTable schemaTable, Long id) {
        this.schemaTable = schemaTable;
        this.id = ID.from(id);
    }

    private RecordId(SchemaTable schemaTable, List<Comparable> identifiers) {
        this.schemaTable = schemaTable;
        this.id = ID.from(identifiers);
    }

    private RecordId(String label, Long id) {
        this.schemaTable = SqlgUtil.parseLabel(label);
        this.id = ID.from(id);
    }

    public static RecordId from(SchemaTable schemaTable, Long id) {
        return new RecordId(schemaTable, id);
    }

    public static RecordId from(SchemaTable schemaTable, List<Comparable> identifiers) {
        return new RecordId(schemaTable, identifiers);
    }

    public static List<RecordId> from(SqlgGraph sqlgGraph, Object... elementId) {
        List<RecordId> result = new ArrayList<>(elementId.length);
        for (Object o : elementId) {
            if (o instanceof RecordId) {
                result.add((RecordId) o);
            } else {
                result.add(RecordId.from(sqlgGraph, o));
            }
        }
        return result;
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

    public static RecordId from(SqlgGraph sqlgGraph, Object vertexId) {
        if (vertexId instanceof Element) {
            return (RecordId) ((SqlgElement) vertexId).id();
        }
        if (vertexId instanceof RecordId) {
            return (RecordId) vertexId;
        }
        if (!(vertexId instanceof String)) {
            throw SqlgExceptions.invalidId(vertexId.toString());
        }
        String stringId = (String) vertexId;
        int indexOf = stringId.indexOf(RECORD_ID_DELIMITER);
        if (indexOf == -1) {
            throw SqlgExceptions.invalidId(vertexId.toString());
        }
        String label = stringId.substring(0, indexOf);
        String id = stringId.substring(indexOf + RECORD_ID_DELIMITER.length());
        if (id.startsWith("[") && id.endsWith("]")) {
            return recordIdFromIdentifiers(sqlgGraph, label, id);
        } else {
            try {
                Long labelId = Long.valueOf(id);
                return new RecordId(label, labelId);
            } catch (NumberFormatException e) {
                throw SqlgExceptions.invalidId(vertexId.toString());
            }
        }
    }

    private static RecordId recordIdFromIdentifiers(SqlgGraph sqlgGraph, String label, String id) {
        SchemaTable schemaTable = SqlgUtil.parseLabel(label);
        id = id.substring(1, id.length() - 1);
        String[] identifiers = id.split(",");
        AbstractLabel abstractLabel;
        Optional<VertexLabel> vertexLabel = sqlgGraph.getTopology().getSchema(schemaTable.getSchema()).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found.", schemaTable.getSchema())))
                .getVertexLabel(schemaTable.getTable());
        if (vertexLabel.isEmpty()) {
            Optional<EdgeLabel> edgeLabel = sqlgGraph.getTopology().getSchema(schemaTable.getSchema()).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found.", schemaTable.getSchema())))
                    .getEdgeLabel(schemaTable.getTable());
            if (edgeLabel.isEmpty()) {
                throw new IllegalStateException(String.format("SchemaTable %s not found", schemaTable));
            }
            abstractLabel = edgeLabel.get();
        } else {
            abstractLabel = vertexLabel.get();
        }
        Preconditions.checkArgument(abstractLabel.getIdentifiers().size() == identifiers.length, "%s identifiers expected in the RecordId. Found %s. given id = %s", abstractLabel.getIdentifiers().size(), identifiers.length, label + id);
        List<Comparable> identifierValues = new ArrayList<>();
        int count = 0;
        for (String identifier : abstractLabel.getIdentifiers()) {
            Optional<PropertyColumn> propertyColumn = abstractLabel.getProperty(identifier);
            if (propertyColumn.isPresent()) {
                PropertyType propertyType = propertyColumn.get().getPropertyType();
                Comparable value = (Comparable) SqlgUtil.stringValueToType(propertyType, identifiers[count++].trim());
                identifierValues.add(value);
            } else {
                throw new IllegalStateException(String.format("identifier %s for %s not found", identifier, schemaTable));
            }
        }
        return RecordId.from(schemaTable, identifierValues);
    }

    public static RecordId from(Object vertexId) {
        if (vertexId instanceof Element) {
            return (RecordId) ((SqlgElement) vertexId).id();
        }
        if (vertexId instanceof RecordId) {
            return (RecordId) vertexId;
        }
        if (!(vertexId instanceof String)) {
            throw SqlgExceptions.invalidId(vertexId.toString());
        }
        String stringId = (String) vertexId;
        String[] splittedId = stringId.split(RECORD_ID_DELIMITER);
        if (splittedId.length == 2) {
            String label = splittedId[0];
            String id = splittedId[1];
            if (id.startsWith("[") && id.endsWith("]")) {
                throw SqlgExceptions.invalidFromRecordId(vertexId.toString());
            } else {
                try {
                    Long labelId = Long.valueOf(id);
                    return new RecordId(label, labelId);
                } catch (NumberFormatException e) {
                    throw SqlgExceptions.invalidId(vertexId.toString());
                }
            }
        } else if (splittedId.length > 2) {
            throw SqlgExceptions.invalidFromRecordId(vertexId.toString());
        } else {
            throw SqlgExceptions.invalidId(vertexId.toString());
        }
    }

    public SchemaTable getSchemaTable() {
        return schemaTable;
    }

    public ID getID() {
        return this.id;
    }

    public List<Comparable> getIdentifiers() {
        return this.id.identifiers;
    }

    public Long sequenceId() {
        return this.id.sequenceId;
    }

    @Override
    public String toString() {
        return this.schemaTable.toString() + RECORD_ID_DELIMITER + this.id.toString();
    }

    @Override
    public int hashCode() {
        int result = this.schemaTable.hashCode();
        return result ^ this.id.hashCode();
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
        return this.schemaTable.equals(otherRecordId.getSchemaTable()) && this.id.equals(otherRecordId.getID());
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(this.getSchemaTable().getSchema());
        output.writeString(this.getSchemaTable().getTable());
        if (hasSequenceId()) {
            output.writeString("s");
            output.writeLong(this.getID().sequenceId);
        } else {
            output.writeString("i");
            output.writeInt(getIdentifiers().size());
            for (Comparable identifier : getIdentifiers()) {
                output.writeString((CharSequence) identifier);
            }
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.schemaTable = SchemaTable.of(input.readString(), input.readString());
        String s = input.readString();
        if (s.equals("s")) {
            //sequence
            this.id = ID.from(input.readLong());
        } else {
            int size = input.readInt();
            List<Comparable> identifiers = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                String identifier = input.readString();
                identifiers.add(identifier);
            }
            this.id = ID.from(identifiers);
        }
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof RecordId)) {
            return -1;
        }
        RecordId other = (RecordId) o;
        int first = this.getSchemaTable().compareTo(other.getSchemaTable());
        if (first != 0) {
            return first;
        }
        return this.getID().compareTo(other.getID());
    }

    public boolean hasSequenceId() {
        return this.id.hasSequenceId();
    }

    public static class RecordIdJacksonSerializerV1d0 extends StdSerializer<RecordId> {
        public RecordIdJacksonSerializerV1d0() {
            super(RecordId.class);
        }

        @Override
        public void serialize(final RecordId recordId, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            // when types are not embedded, stringify or resort to JSON primitive representations of the
            // type so that non-jvm languages can better interoperate with the TinkerPop stack.
            jsonGenerator.writeString(recordId.toString());
        }

        @Override
        public void serializeWithType(final RecordId recordId, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {

            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(GraphSONTokens.CLASS, RecordId.class.getName());
            jsonGenerator.writeObjectField("schemaTable", recordId.getSchemaTable());
            jsonGenerator.writeNumberField("id", recordId.sequenceId());
            jsonGenerator.writeEndObject();
        }
    }

    static class RecordIdJacksonDeserializerV1d0 extends StdDeserializer<RecordId> {
        RecordIdJacksonDeserializerV1d0() {
            super(RecordId.class);
        }

        @Override
        public RecordId deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
            JsonToken jsonToken = jsonParser.nextToken();
            Preconditions.checkState(JsonToken.START_OBJECT == jsonToken);
            SchemaTable schemaTable = deserializationContext.readValue(jsonParser, SchemaTable.class);
            jsonToken = jsonParser.nextToken();
            Preconditions.checkState(org.apache.tinkerpop.shaded.jackson.core.JsonToken.FIELD_NAME == jsonToken);
            Preconditions.checkState("id".equals(jsonParser.getValueAsString()));
            jsonToken = jsonParser.nextToken();
            Preconditions.checkState(JsonToken.VALUE_NUMBER_INT == jsonToken);
            long id = jsonParser.getValueAsLong();
            jsonToken = jsonParser.nextToken();
            Preconditions.checkState(org.apache.tinkerpop.shaded.jackson.core.JsonToken.END_OBJECT == jsonToken);
            return RecordId.from(schemaTable, id);
        }

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

        @SuppressWarnings("deprecation")
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
            if (recordId.hasSequenceId()) {
                m.put("id", recordId.sequenceId());
            } else {
                m.put("id", recordId.getIdentifiers());
            }
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
            SchemaTable schemaTable = (SchemaTable) data.get("schemaTable");
            if (data.get("id") instanceof Long) {
                return RecordId.from(schemaTable, (Long) data.get("id"));
            } else {
                @SuppressWarnings("unchecked")
                List<Comparable> identifiers = (List<Comparable>) data.get("id");
                return RecordId.from(schemaTable, identifiers);
            }
        }
    }

    static class RecordIdJacksonSerializerV3d0 extends StdScalarSerializer<RecordId> {
        @SuppressWarnings("WeakerAccess")
        public RecordIdJacksonSerializerV3d0() {
            super(RecordId.class);
        }

        @Override
        public void serialize(final RecordId recordId, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            final Map<String, Object> m = new HashMap<>();
            m.put("schemaTable", recordId.getSchemaTable());
            if (recordId.hasSequenceId()) {
                m.put("id", recordId.sequenceId());
            } else {
                m.put("id", recordId.getIdentifiers());
            }
            jsonGenerator.writeObject(m);
        }

    }

    static class RecordIdJacksonDeserializerV3d0 extends StdDeserializer<RecordId> {
        @SuppressWarnings("WeakerAccess")
        public RecordIdJacksonDeserializerV3d0() {
            super(RecordId.class);
        }

        @Override
        public RecordId deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
            @SuppressWarnings("unchecked") final Map<String, Object> data = deserializationContext.readValue(jsonParser, Map.class);
            SchemaTable schemaTable = (SchemaTable) data.get("schemaTable");
            if (data.get("id") instanceof Long) {
                return RecordId.from(schemaTable, (Long) data.get("id"));
            } else {
                @SuppressWarnings("unchecked")
                List<Comparable> identifiers = new ArrayList<>((List<Comparable>) data.get("id"));
                return RecordId.from(schemaTable, identifiers);
            }
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    public static final class ID implements Comparable<ID> {

        private Long sequenceId;
        private List<Comparable> identifiers;

        private ID(List<Comparable> identifiers) {
            this.identifiers = identifiers;
        }

        private ID(Long id) {
            this.sequenceId = id;
        }

        static ID from(Long sequenceId) {
            return new ID(sequenceId);
        }

        static ID from(List<Comparable> identifiers) {
            return new ID(identifiers);
        }

        @Override
        public int compareTo(ID id) {
            if (this.sequenceId != null) {
                return this.sequenceId.compareTo(id.sequenceId);
            } else {
                int count = 0;
                for (Comparable identifier : identifiers) {
                    @SuppressWarnings("unchecked")
                    int i = identifier.compareTo(id.identifiers.get(count++));
                    if (i != 0) {
                        return i;
                    }
                }
            }
            return 0;
        }

        @Override
        public String toString() {
            return this.sequenceId != null ? this.sequenceId.toString() : this.identifiers.toString();
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ID)) {
                return false;
            }
            ID otherID = (ID) other;
            if (this.sequenceId != null) {
                return this.sequenceId.equals(otherID.sequenceId);
            } else {
                return this.identifiers.equals(otherID.identifiers);
            }
        }

        @Override
        public int hashCode() {
            if (this.sequenceId != null) {
                return this.sequenceId.hashCode();
            } else {
                StringBuilder sb = new StringBuilder();
                for (Object identifier : this.identifiers) {
                    sb.append(identifier.toString());
                }
                return sb.toString().hashCode();
            }
        }

        public boolean hasSequenceId() {
            return this.sequenceId != null;
        }

        public Long getSequenceId() {
            return sequenceId;
        }

        public List<Comparable> getIdentifiers() {
            return identifiers;
        }
    }

}
