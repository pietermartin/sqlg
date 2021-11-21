package org.umlg.sqlg.structure.io.binary;

import org.apache.commons.lang3.SerializationException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.CustomTypeSerializer;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SchemaTable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RecordIdBinarySerializer implements CustomTypeSerializer<RecordId> {

    private final byte[] typeInfoBuffer = new byte[] { 0, 0, 0, 0 };

    @Override
    public String getTypeName() {
        return "sqlg.RecordId";
    }

    @Override
    public DataType getDataType() {
        return DataType.CUSTOM;
    }

    @Override
    public RecordId read(Buffer buffer, GraphBinaryReader context) throws IOException {
        // {custom type info}, {value_flag} and {value}
        // No custom_type_info
        if (buffer.readInt() != 0) {
            throw new SerializationException("{custom_type_info} should not be provided for this custom type");
        }

        return readValue(buffer, context, true);
    }

    @Override
    public RecordId readValue(Buffer buffer, GraphBinaryReader context, boolean nullable) throws IOException {
        if (nullable) {
            final byte valueFlag = buffer.readByte();
            if ((valueFlag & 1) == 1) {
                return null;
            }
        }

        // Read the byte length of the value bytes
        final int valueLength = buffer.readInt();

        if (valueLength <= 0) {
            throw new SerializationException(String.format("Unexpected value length: %d", valueLength));
        }

        if (valueLength > buffer.readableBytes()) {
            throw new SerializationException(
                    String.format("Not enough readable bytes: %d (expected %d)", valueLength, buffer.readableBytes()));
        }

        final String schemaTable = context.readValue(buffer, String.class, false);
        int indexOf = schemaTable.indexOf(".");
        String schema = schemaTable.substring(0, indexOf);
        String table = schemaTable.substring(indexOf + 1);
        final Long id = context.readValue(buffer, Long.class, false);

        return RecordId.from(SchemaTable.of(schema, table), id);
    }

    @Override
    public void write(RecordId recordId, Buffer buffer, GraphBinaryWriter context) throws IOException {
        // Write {custom type info}, {value_flag} and {value}
        buffer.writeBytes(typeInfoBuffer);

        writeValue(recordId, buffer, context, true);
    }

    @Override
    public void writeValue(RecordId recordId, Buffer buffer, GraphBinaryWriter context, boolean nullable) throws IOException {
        if (recordId == null) {
            if (!nullable) {
                throw new SerializationException("Unexpected null value when nullable is false");
            }

            context.writeValueFlagNull(buffer);
            return;
        }

        if (nullable) {
            context.writeValueFlagNone(buffer);
        }

        final String schemaTable = recordId.getSchemaTable().toString();

        // value_length = name_byte_length + name_bytes + long
        buffer.writeInt(4 + schemaTable.getBytes(StandardCharsets.UTF_8).length + 8);

        context.writeValue(schemaTable, buffer, false);
        context.writeValue(recordId.getID().getSequenceId(), buffer, false);
    }
}
