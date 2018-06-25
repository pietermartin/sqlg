package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

/**
 * Date: 2015/05/07
 * Time: 8:05 PM
 */
public class SqlgIoRegistryV2 extends AbstractIoRegistry {

    private static final SqlgIoRegistryV2 INSTANCE = new SqlgIoRegistryV2();

    private SqlgIoRegistryV2() {
        final SqlgSimpleModuleV2 sqlgSimpleModuleV2 = new SqlgSimpleModuleV2();
        register(GraphSONIo.class, null, sqlgSimpleModuleV2);
        register(GryoIo.class, RecordId.class, null);
    }

    public static SqlgIoRegistryV2 instance() {
        return INSTANCE;
    }
}
