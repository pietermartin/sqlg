package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

/**
 * Date: 2015/05/07
 * Time: 8:05 PM
 */
public class SqlgIoRegistry extends AbstractIoRegistry {

    public SqlgIoRegistry() {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(RecordId.class, new RecordId.RecordIdJacksonSerializer());
        module.addSerializer(SchemaTable.class, new SchemaTable.SchemaTableJacksonSerializer());
        register(GraphSONIo.class, null, module);
        register(GryoIo.class, RecordId.class, null);
    }
}
