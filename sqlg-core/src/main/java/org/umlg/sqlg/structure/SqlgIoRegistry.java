package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

/**
 * Date: 2015/05/07
 * Time: 8:05 PM
 */
public class SqlgIoRegistry extends AbstractIoRegistry {

    private static final SqlgIoRegistry INSTANCE = new SqlgIoRegistry();

    private SqlgIoRegistry() {
        final SqlgSimpleModule sqlgSimpleModule = new SqlgSimpleModule();
        register(GraphSONIo.class, null, sqlgSimpleModule);
        register(GryoIo.class, RecordId.class, null);
    }

    public static SqlgIoRegistry getInstance() {
        return INSTANCE;
    }
}
