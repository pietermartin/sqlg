package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;

/**
 * Date: 2014/07/13
 * Time: 6:32 PM
 */
public class TinkerpopTest extends BaseTest {

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_2X_capXaX() throws IOException {
        readGraphMLIntoGraph(this.sqlG);

        Traversal<Vertex, Map<String, Integer>> traversal = get_g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_2X_capXaX();
        printTraversalForm(traversal);
        final Map<String, Integer> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(4, map.size());
        assertTrue(map.containsKey("vadas"));
        assertEquals(Integer.valueOf(1), map.get("vadas"));
        assertTrue(map.containsKey("josh"));
        assertEquals(Integer.valueOf(1), map.get("josh"));
        assertTrue(map.containsKey("lop"));
        assertEquals(Integer.valueOf(4), map.get("lop"));
        assertTrue(map.containsKey("ripple"));
        assertEquals(Integer.valueOf(2), map.get("ripple"));

    }

    public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_2X_capXaX() {
        return this.sqlG.V().as("x").out().groupBy(v -> v.value("name"), v -> v, vv -> vv.size()).as("a").jump("x", 2).cap("a");
    }

    public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_loops_lt_2X_capXaX() {
        return this.sqlG.V().as("x").out().groupBy(v -> v.value("name"), v -> v, vv -> vv.size()).as("a").jump("x", t -> t.getLoops() < 2).cap("a");
    }

    public Traversal<Vertex, Edge> get_g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX(final Object v1Id) {
        return this.sqlG.v(v1Id).outE("knows").has("weight", 1.0f).as("here").inV().has("name", "josh").back("here");
    }

    protected void printTraversalForm(final Traversal traversal) {
        System.out.println("Testing: " + traversal);
        traversal.strategies().apply();
        System.out.println("         " + traversal);
    }

    private static void readGraphMLIntoGraph(final Graph g) throws IOException {
        final GraphReader reader = GraphMLReader.build().create();
        try (final InputStream stream = new FileInputStream(new File("sqlg-test/src/main/resources/tinkerpop-classic.xml"))) {
            reader.readGraph(stream, g);
        }
//        try (final InputStream stream = TinkerpopTest.class.getResourceAsStream("tinkerpop-classic.xml")) {
//            reader.readGraph(stream, g);
//        }
    }

}
