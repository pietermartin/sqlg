package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

/**
 * Date: 2015/05/07
 * Time: 8:05 PM
 */
public class SqlgIoRegistryV1 extends AbstractIoRegistry {

    private static final SqlgIoRegistryV1 INSTANCE = new SqlgIoRegistryV1();

    private SqlgIoRegistryV1() {
        final SqlgSimpleModuleV1 sqlgSimpleModuleV1 = new SqlgSimpleModuleV1();
        register(GraphSONIo.class, null, sqlgSimpleModuleV1);
        register(GryoIo.class, RecordId.class, null);
    }

    public static SqlgIoRegistryV1 instance() {
        return INSTANCE;
    }
}
