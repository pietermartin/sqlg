package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.umlg.sqlg.structure.io.binary.RecordIdBinarySerializer;

/**
 * Date: 2015/05/07
 * Time: 8:05 PM
 */
public class SqlgIoRegistryV3 extends AbstractIoRegistry {

    private static final SqlgIoRegistryV3 INSTANCE = new SqlgIoRegistryV3();

    private SqlgIoRegistryV3() {
        final SqlgSimpleModuleV3 sqlgSimpleModule = new SqlgSimpleModuleV3();
        register(GraphSONIo.class, null, sqlgSimpleModule);
        register(GryoIo.class, RecordId.class, null);
        register(GraphBinaryIo.class, RecordId.class, new RecordIdBinarySerializer());
    }

    public static SqlgIoRegistryV3 instance() {
        return INSTANCE;
    }
}
