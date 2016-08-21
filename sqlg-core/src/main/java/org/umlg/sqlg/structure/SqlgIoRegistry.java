package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;

/**
 * Date: 2015/05/07
 * Time: 8:05 PM
 */
class SqlgIoRegistry extends AbstractIoRegistry {

    private static final SqlgIoRegistry INSTANCE = new SqlgIoRegistry();

    private SqlgIoRegistry() {
        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(RecordId.class, new RecordId.RecordIdJacksonSerializer());
        simpleModule.addSerializer(SchemaTable.class, new SchemaTable.SchemaTableJacksonSerializer());
        register(GraphSONIo.class, null, simpleModule);
        register(GryoIo.class, RecordId.class, null);
    }

    public static SqlgIoRegistry getInstance() {
        return INSTANCE;
    }
}
