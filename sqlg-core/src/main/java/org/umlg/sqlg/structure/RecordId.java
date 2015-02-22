package org.umlg.sqlg.structure;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 2015/02/21
 * Time: 8:50 PM
 */
public class RecordId {

    public final static String RECORD_ID_DELIMITER = ":::";
    private SchemaTable schemaTable;
    private Long id;

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
}
