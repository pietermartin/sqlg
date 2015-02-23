package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.DefaultIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.kryo.KryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import org.apache.tinkerpop.gremlin.structure.io.kryo.KryoWriter;

/**
 * Date: 2015/02/23
 * Time: 4:35 PM
 */
public class SqlgIo extends DefaultIo implements Graph.Io {

    private final KryoMapper kryoMapper;

    public SqlgIo(final Graph g) {
        super(g);
        this.kryoMapper = KryoMapper.build().addCustom(RecordId.class).create();
    }

    @Override
    public KryoWriter.Builder kryoWriter() {
        return KryoWriter.build().mapper(this.kryoMapper);
    }

    @Override
    public KryoReader.Builder kryoReader() {
        return KryoReader.build().mapper(this.kryoMapper);
    }

    @Override
    public GraphSONReader.Builder graphSONReader() {
//        final SimpleModule module = new SimpleModule();
//        module.addSerializer(RecordId.class, new RecordId.CustomIdJacksonSerializer());
//        module.addDeserializer(RecordId.class, new RecordId.CustomIdJacksonDeserializer());
//        return GraphSONReader.build().mapper(graphSONMapper().addCustomModule(module).embedTypes(true).create());
        return GraphSONReader.build().mapper(graphSONMapper().create());
    }

    @Override
    public GraphSONWriter.Builder graphSONWriter() {
//        final SimpleModule module = new SimpleModule();
//        module.addSerializer(RecordId.class, new RecordId.CustomIdJacksonSerializer());
//        module.addDeserializer(RecordId.class, new RecordId.CustomIdJacksonDeserializer());
//        return GraphSONWriter.build().mapper(graphSONMapper().addCustomModule(module).embedTypes(true).create());
        return GraphSONWriter.build().mapper(graphSONMapper().create());
    }

    @Override
    public GraphSONMapper.Builder graphSONMapper() {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(RecordId.class, new RecordId.CustomIdJacksonSerializer());
        module.addDeserializer(RecordId.class, new RecordId.CustomIdJacksonDeserializer());
        return GraphSONMapper.build().addCustomModule(module).embedTypes(true);
    }

}
