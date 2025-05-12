package org.umlg.sqlg.services;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.util.CollectionUtil.asMap;

public class SqlgPGVectorFactory extends SqlgServiceRegistry.SqlgServiceFactory<Vertex, Long> implements Service<Vertex, Long> {

    public static final String NAME = "sqlg.pgvector";
    public static final String l2distance = "l2distance";

    public interface Params {
        String FUNCTION = "function";
        String SOURCE = "source";
        String TARGET = "target";

        Map DESCRIBE = asMap(
                SqlgPGVectorFactory.Params.FUNCTION, "The pg_routing function",
                Params.SOURCE, "the vector to measure the distance from.",
                Params.TARGET, "the vector to measure the distance to."
        );
    }

    public SqlgPGVectorFactory(final SqlgGraph graph) {
        super(graph, NAME);
    }

    @Override
    public Type getType() {
        return Type.Streaming;
    }

    @Override
    public Set<Type> getSupportedTypes() {
        return Collections.singleton(Type.Streaming);
    }

    @Override
    public Service<Vertex, Long> createService(boolean isStart, Map params) {
        if (isStart) {
            throw new UnsupportedOperationException(Exceptions.cannotStartTraversal);
        }
        return this;
    }

    @Override
    public CloseableIterator<Long> execute(final ServiceCallContext ctx, final Traverser.Admin<Vertex> in, final Map params) {
        throw new UnsupportedOperationException("CallStep should have been replaced.");
    }

    @Override
    public void close() {
    }
}
