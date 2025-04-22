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

public class SqlgPGRoutingFactory  extends SqlgServiceRegistry.SqlgServiceFactory<Vertex,Long> implements Service<Vertex, Long> {

    public static final String NAME = "sqlg.pgrouting";
    public static final String pgr_dijkstra = "pgr_dijkstra";

    public interface Params {
        String FUNCTION = "function";
        String DIRECTED = "directed";
        String START_VID = "start_vid";
        String START_VIDS = "start_vids";
        String END_VID = "end_vid";
        String END_VIDS = "end_vids";

        Map DESCRIBE = asMap(
                SqlgPGRoutingFactory.Params.FUNCTION, "The pg_routing function",
                SqlgPGRoutingFactory.Params.START_VID, "Identifier of the starting vertex of the path.",
                SqlgPGRoutingFactory.Params.START_VIDS, "Array of identifiers of starting vertices.",
                SqlgPGRoutingFactory.Params.END_VID, "Identifier of the ending vertex of the path.",
                SqlgPGRoutingFactory.Params.END_VIDS, "Array of identifiers of ending vertices.",
                SqlgPGRoutingFactory.Params.DIRECTED, "When true the graph is considered Directed, When false the graph is considered as Undirected."
        );
    }

    public SqlgPGRoutingFactory(final SqlgGraph graph) {
        super(graph, NAME);
    }

    @Override
    public Type getType() {
        return Type.Streaming;
    }

    @Override
    public Map describeParams() {
        return SqlgDegreeCentralityFactory.Params.DESCRIBE;
    }

    @Override
    public Set<Type> getSupportedTypes() {
        return Collections.singleton(Type.Streaming);
    }

    @Override
    public Service<Vertex, Long> createService(final boolean isStart, final Map params) {
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
    public void close() {}
}
