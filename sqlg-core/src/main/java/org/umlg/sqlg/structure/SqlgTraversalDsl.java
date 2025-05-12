package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.GremlinDsl;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.umlg.sqlg.services.SqlgPGRoutingFactory;

import java.util.List;
import java.util.Map;

@GremlinDsl
public interface SqlgTraversalDsl<S, E> extends GraphTraversal.Admin<S, E> {

    /**
     *
     * @param startVid sequence id of the start vertex
     * @param endVid sequence id of the end vertex
     * @param directed is the graph directed or not
     * @return a @{@link Path} from startVid to endVid
     */
    public default GraphTraversal<S, Path> dijkstra(long startVid, long endVid, boolean directed) {
        return call(
                SqlgPGRoutingFactory.NAME,
                Map.of(
                        SqlgPGRoutingFactory.Params.FUNCTION, SqlgPGRoutingFactory.pgr_dijkstra,
                        SqlgPGRoutingFactory.Params.START_VID, startVid,
                        SqlgPGRoutingFactory.Params.END_VID, endVid,
                        SqlgPGRoutingFactory.Params.DIRECTED, directed
                )
        );
    }

    public default GraphTraversal<S, Path> dijkstra(List<Long> startVids, List<Long> endVids, boolean directed) {
        return call(
                SqlgPGRoutingFactory.NAME,
                Map.of(
                        SqlgPGRoutingFactory.Params.FUNCTION, SqlgPGRoutingFactory.pgr_dijkstra,
                        SqlgPGRoutingFactory.Params.START_VIDS, startVids,
                        SqlgPGRoutingFactory.Params.END_VIDS, endVids,
                        SqlgPGRoutingFactory.Params.DIRECTED, directed
                )
        );
    }

    public default GraphTraversal<S, Path> dijkstra(long startVid, List<Long> endVids, boolean directed) {
        return call(
                SqlgPGRoutingFactory.NAME,
                Map.of(
                        SqlgPGRoutingFactory.Params.FUNCTION, SqlgPGRoutingFactory.pgr_dijkstra,
                        SqlgPGRoutingFactory.Params.START_VID, startVid,
                        SqlgPGRoutingFactory.Params.END_VIDS, endVids,
                        SqlgPGRoutingFactory.Params.DIRECTED, directed
                )
        );
    }

    public default GraphTraversal<S, Path> dijkstra(List<Long> startVids, long endVid, boolean directed) {
        return call(
                SqlgPGRoutingFactory.NAME,
                Map.of(
                        SqlgPGRoutingFactory.Params.FUNCTION, SqlgPGRoutingFactory.pgr_dijkstra,
                        SqlgPGRoutingFactory.Params.START_VIDS, startVids,
                        SqlgPGRoutingFactory.Params.END_VID, endVid,
                        SqlgPGRoutingFactory.Params.DIRECTED, directed
                )
        );
    }

}
