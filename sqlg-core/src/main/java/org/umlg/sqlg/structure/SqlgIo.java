package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.DefaultIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.kryo.KryoMapper;

/**
 * Date: 2015/02/23
 * Time: 4:35 PM
 */
public class SqlgIo extends DefaultIo implements Graph.Io {

    public SqlgIo(final Graph g) {
        super(g);
    }

    @Override
    public KryoMapper.Builder kryoMapper() {
        return KryoMapper.build().addCustom(RecordId.class);
    }

    @Override
    public GraphSONMapper.Builder graphSONMapper() {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(RecordId.class, new RecordId.RecordIdJacksonSerializer());
        module.addDeserializer(RecordId.class, new RecordId.RecordIdJacksonDeserializer());
        return GraphSONMapper.build().addCustomModule(module);
//        return GraphSONMapper.build().addCustomModule(module).embedTypes(true);
    }

}
