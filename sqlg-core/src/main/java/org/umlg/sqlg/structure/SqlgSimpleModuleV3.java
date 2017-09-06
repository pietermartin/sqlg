package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Date: 2016/08/28
 * Time: 12:40 PM
 */
class SqlgSimpleModuleV3 extends TinkerPopJacksonModule {

    private static final Map<Class, String> TYPE_DEFINITIONS = Collections.unmodifiableMap(
            new LinkedHashMap<Class, String>() {{
                put(RecordId.class, "id");
                put(SchemaTable.class, "schemaTable");
            }});

    SqlgSimpleModuleV3() {
        super("custom");
        addSerializer(RecordId.class, new RecordId.RecordIdJacksonSerializerV3d0());
        addDeserializer(RecordId.class, new RecordId.RecordIdJacksonDeserializerV3d0());
        addSerializer(SchemaTable.class, new SchemaTable.SchemaTableJacksonSerializerV3d0());
        addDeserializer(SchemaTable.class, new SchemaTable.SchemaTableJacksonDeserializerV3d0());
    }

    @Override
    public Map<Class, String> getTypeDefinitions() {
        return TYPE_DEFINITIONS;
    }

    @Override
    public String getTypeNamespace() {
        return "simple";
    }
}
