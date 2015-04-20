package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.DefaultIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;

/**
 * Date: 2015/02/23
 * Time: 4:35 PM
 */
public class SqlgIo extends DefaultIo implements Graph.Io {

    public SqlgIo(final Graph g) {
        super(g);
    }

    @Override
    public GryoMapper.Builder gryoMapper() {
        return GryoMapper.build().addCustom(RecordId.class);
    }

    @Override
    public GraphSONMapper.Builder graphSONMapper() {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(RecordId.class, new RecordId.RecordIdJacksonSerializer());
        module.addSerializer(SchemaTable.class, new SchemaTable.SchemaTableJacksonSerializer());
        return GraphSONMapper.build().addCustomModule(module);
    }

}
