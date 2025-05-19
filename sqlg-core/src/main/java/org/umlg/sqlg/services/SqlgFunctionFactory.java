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

public class SqlgFunctionFactory  extends SqlgServiceRegistry.SqlgServiceFactory<Vertex, Long> implements Service<Vertex, Long> {

    public static final String NAME = "sqlg.functions";

    public interface Params {
        String RESUL_PROPERTY_TYPE= "resultPropertyType";
        String COLUMN_NAME = "columnName";
        String FUNCTION_AS_STRING = "functionAsString";

        Map DESCRIBE = asMap(
                Params.FUNCTION_AS_STRING, "Sqlg functions service",
                Params.COLUMN_NAME, "The name of the column holding the result of the function",
                Params.RESUL_PROPERTY_TYPE, "The type of the result property."
        );
    }

    public SqlgFunctionFactory(final SqlgGraph graph) {
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
