package org.umlg.sqlg.test.pgrouting;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.services.SqlgPGRoutingFactory;
import org.umlg.sqlg.structure.SqlgTraversalSource;

import java.util.List;
import java.util.Map;

public class PGRDijkstraWikiGraphTest extends BasePGRouting {

    private final static Logger LOGGER = LoggerFactory.getLogger(PGRDijkstraWikiGraphTest.class);

    @Test
    public void g_V_call_dijkstra() {
        loadWikiGraph();

        List<Path> result1 = sqlgGraph.traversal(SqlgTraversalSource.class).E()
                .hasLabel("connects")
                .dijkstra(1L, 5L, false)
                .toList();

        List<Path> result2 = sqlgGraph.traversal().E().hasLabel("connects").<Path>call(
                        SqlgPGRoutingFactory.NAME,
                        Map.of(
                                SqlgPGRoutingFactory.Params.FUNCTION, SqlgPGRoutingFactory.pgr_dijkstra,
                                SqlgPGRoutingFactory.Params.START_VID, 1L,
                                SqlgPGRoutingFactory.Params.END_VID, 5L,
                                SqlgPGRoutingFactory.Params.DIRECTED, false
                        ))
                .toList();

        Assert.assertEquals(result1, result2);
        List<Path> result = result1;

        Assert.assertEquals(1, result.size());
        Path p = result.get(0);
        LOGGER.debug(p.toString());
        Assert.assertTrue(p.get(1) instanceof Edge && ((Edge) p.get(1)).value("source").equals(1) && ((Edge) p.get(1)).value("target").equals(3));
        Assert.assertTrue(p.get(3) instanceof Edge && ((Edge) p.get(3)).value("source").equals(3) && ((Edge) p.get(3)).value("target").equals(6));
        Assert.assertTrue(p.get(5) instanceof Edge && ((Edge) p.get(5)).value("source").equals(5) && ((Edge) p.get(5)).value("target").equals(6));
        Assert.assertTrue(
                p.size() == 7 &&
                        p.get(0) instanceof Vertex && ((Vertex) p.get(0)).label().equals("wiki") && ((Vertex) p.get(0)).value("name").equals("1") && ((Vertex) p.get(0)).value("_index").equals(1) &&
                        p.get(1) instanceof Edge && ((Edge) p.get(1)).label().equals("connects") && ((Edge) p.get(1)).value("source").equals(1) && ((Edge) p.get(1)).value("target").equals(3) &&
                        p.get(2) instanceof Vertex && ((Vertex) p.get(0)).label().equals("wiki") && ((Vertex) p.get(2)).value("name").equals("3") && ((Vertex) p.get(2)).value("_index").equals(3) &&
                        p.get(3) instanceof Edge && ((Edge) p.get(1)).label().equals("connects") && ((Edge) p.get(3)).value("source").equals(3) && ((Edge) p.get(3)).value("target").equals(6) &&
                        p.get(4) instanceof Vertex && ((Vertex) p.get(0)).label().equals("wiki") && ((Vertex) p.get(4)).value("name").equals("6") && ((Vertex) p.get(4)).value("_index").equals(6) &&
                        p.get(5) instanceof Edge && ((Edge) p.get(1)).label().equals("connects") && ((Edge) p.get(5)).value("source").equals(5) && ((Edge) p.get(5)).value("target").equals(6) &&
                        p.get(6) instanceof Vertex && ((Vertex) p.get(0)).label().equals("wiki") && ((Vertex) p.get(6)).value("name").equals("5") && ((Vertex) p.get(6)).value("_index").equals(5)
        );
    }

}
