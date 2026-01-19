package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.GremlinDsl;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlg.services.SqlgFunctionFactory;
import org.umlg.sqlg.services.SqlgPGRoutingFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

@SuppressWarnings("resource")
@GremlinDsl
public interface SqlgTraversalDsl<S, E> extends GraphTraversal.Admin<S, E> {

    /**
     * Implements <a href="https://docs.pgrouting.org/latest/en/pgr_connectedComponents.html">pgr_connectedComponents</a>.
     * The methods runs over the whole graph.
     * @return Returns each vertex in the graph, with a special property `SqlgPGRoutingFactory.TRAVERSAL_COMPONENT` which holds
     * the component label.
     */
    default GraphTraversal<S, Vertex> pgrConnectedComponent() {
        return call(
                SqlgPGRoutingFactory.NAME,
                Map.of(
                        SqlgPGRoutingFactory.Params.FUNCTION, SqlgPGRoutingFactory.pgr_connectedComponents
                )
        );
    }

    /**
     * @param startVid sequence id of the start vertex
     * @param endVid   sequence id of the end vertex
     * @param directed is the graph directed or not
     * @return a @{@link Path} from startVid to endVid
     */
    default GraphTraversal<S, Path> dijkstra(long startVid, long endVid, boolean directed) {
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

    default GraphTraversal<S, Path> dijkstra(List<Long> startVids, List<Long> endVids, boolean directed) {
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

    default GraphTraversal<S, Path> dijkstra(long startVid, List<Long> endVids, boolean directed) {
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

    default GraphTraversal<S, Path> dijkstra(List<Long> startVids, long endVid, boolean directed) {
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

    default <R> GraphTraversal<S, R> fun(String columnName, PropertyType functionResultPropertyType, Function<Object, String> fun) {
        return call(
                SqlgFunctionFactory.NAME,
                Map.of(
                        SqlgFunctionFactory.Params.COLUMN_NAME, columnName,
                        SqlgFunctionFactory.Params.RESUL_PROPERTY_TYPE, functionResultPropertyType,
                        SqlgFunctionFactory.Params.FUNCTION_AS_STRING, fun
                )
        );
    }

    default <R> GraphTraversal<S, R> l2distance(String columnName, String source, float[] target) {
        SqlgGraph sqlgGraph = (SqlgGraph) getGraph().orElseThrow();
        String targetVectorAsString = sqlgGraph.getSqlDialect().toRDBSStringLiteral(target);
        Function<Object, String> fun = o -> "(\"" + source + "\" <-> " + targetVectorAsString + ")";
        return call(
                SqlgFunctionFactory.NAME,
                Map.of(
                        SqlgFunctionFactory.Params.COLUMN_NAME, columnName,
                        SqlgFunctionFactory.Params.RESUL_PROPERTY_TYPE, PropertyType.DOUBLE,
                        SqlgFunctionFactory.Params.FUNCTION_AS_STRING, fun
                )
        );
    }

    default <R> GraphTraversal<S, R> l1distance(String columnName, String source, float[] target) {
        SqlgGraph sqlgGraph = (SqlgGraph) getGraph().orElseThrow();
        String targetVectorAsString = sqlgGraph.getSqlDialect().toRDBSStringLiteral(target);
        Function<Object, String> fun = o -> "(\"" + source + "\" <+> " + targetVectorAsString + ")";
        return call(
                SqlgFunctionFactory.NAME,
                Map.of(
                        SqlgFunctionFactory.Params.COLUMN_NAME, columnName,
                        SqlgFunctionFactory.Params.RESUL_PROPERTY_TYPE, PropertyType.DOUBLE,
                        SqlgFunctionFactory.Params.FUNCTION_AS_STRING, fun
                )
        );
    }

    default <R> GraphTraversal<S, R> innerProduct(String columnName, String source, float[] target) {
        SqlgGraph sqlgGraph = (SqlgGraph) getGraph().orElseThrow();
        String targetVectorAsString = sqlgGraph.getSqlDialect().toRDBSStringLiteral(target);
        Function<Object, String> fun = o -> "((\"" + source + "\" <#> " + targetVectorAsString + ") * -1)";
        return call(
                SqlgFunctionFactory.NAME,
                Map.of(
                        SqlgFunctionFactory.Params.COLUMN_NAME, columnName,
                        SqlgFunctionFactory.Params.RESUL_PROPERTY_TYPE, PropertyType.DOUBLE,
                        SqlgFunctionFactory.Params.FUNCTION_AS_STRING, fun
                )
        );
    }

    default <R> GraphTraversal<S, R> cosineDistance(String columnName, String source, float[] target) {
        SqlgGraph sqlgGraph = (SqlgGraph) getGraph().orElseThrow();
        String targetVectorAsString = sqlgGraph.getSqlDialect().toRDBSStringLiteral(target);
        Function<Object, String> fun = o -> "(1 - (\"" + source + "\" <=> " + targetVectorAsString + "))";
        return call(
                SqlgFunctionFactory.NAME,
                Map.of(
                        SqlgFunctionFactory.Params.COLUMN_NAME, columnName,
                        SqlgFunctionFactory.Params.RESUL_PROPERTY_TYPE, PropertyType.DOUBLE,
                        SqlgFunctionFactory.Params.FUNCTION_AS_STRING, fun
                )
        );
    }

    default <R> GraphTraversal<S, R> hammingDistance(String columnName, String source, boolean[] target) {
        StringBuilder targetVectorAsString = new StringBuilder();
        for (boolean b : target) {
            targetVectorAsString.append(b ? "1" : "0");
        }
        Function<Object, String> fun = o -> "(\"" + source + "\" <~> '" + targetVectorAsString + "')";
        return call(
                SqlgFunctionFactory.NAME,
                Map.of(
                        SqlgFunctionFactory.Params.COLUMN_NAME, columnName,
                        SqlgFunctionFactory.Params.RESUL_PROPERTY_TYPE, PropertyType.DOUBLE,
                        SqlgFunctionFactory.Params.FUNCTION_AS_STRING, fun
                )
        );
    }

    default <R> GraphTraversal<S, R> jaccardDistance(String columnName, String source, boolean[] target) {
        StringBuilder targetVectorAsString = new StringBuilder();
        for (boolean b : target) {
            targetVectorAsString.append(b ? "1" : "0");
        }
        Function<Object, String> fun = o -> "(\"" + source + "\" <%> '" + targetVectorAsString + "')";
        return call(
                SqlgFunctionFactory.NAME,
                Map.of(
                        SqlgFunctionFactory.Params.COLUMN_NAME, columnName,
                        SqlgFunctionFactory.Params.RESUL_PROPERTY_TYPE, PropertyType.DOUBLE,
                        SqlgFunctionFactory.Params.FUNCTION_AS_STRING, fun
                )
        );
    }

}
