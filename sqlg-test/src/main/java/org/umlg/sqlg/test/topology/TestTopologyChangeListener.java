package org.umlg.sqlg.test.topology;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.TopologyChangeAction;
import org.umlg.sqlg.structure.TopologyInf;
import org.umlg.sqlg.structure.TopologyListener;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Date: 2017/01/22
 * Time: 6:58 PM
 */
public class TestTopologyChangeListener extends BaseTest {

    private final List<Triple<TopologyInf, TopologyInf, TopologyChangeAction>> topologyListenerTriple = new ArrayList<>();

    @Before
    public void before() throws Exception {
        super.before();
        this.topologyListenerTriple.clear();
    }

    @Test
    public void testAddSchemaAndVertexAndEdge() {
        TopologyListenerTest topologyListenerTest = new TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "asda");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "asdasd");
        Edge e1 = a1.addEdge("aa", a2);
        a1.property("surname", "asdasd");
        e1.property("special", "");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "A.B", "name", "asdasd");
        a1.addEdge("aa", b1);

        Schema schema = this.sqlgGraph.getTopology().getSchema("A").orElseThrow();
        VertexLabel aVertexLabel = schema.getVertexLabel("A").orElseThrow();
        EdgeLabel edgeLabel = aVertexLabel.getOutEdgeLabel("aa").orElseThrow();
        PropertyColumn vertexPropertyColumn = aVertexLabel.getProperty("surname").orElseThrow();
        PropertyColumn edgePropertyColumn = edgeLabel.getProperty("special").orElseThrow();
        VertexLabel bVertexLabel = schema.getVertexLabel("B").orElseThrow();

        Index index = aVertexLabel.ensureIndexExists(IndexType.UNIQUE, new ArrayList<>(aVertexLabel.getProperties().values()));

        assertEquals(8, this.topologyListenerTriple.size());

        assertEquals(schema, this.topologyListenerTriple.get(0).getLeft());
        assertNull(this.topologyListenerTriple.get(0).getMiddle());
        assertEquals(TopologyChangeAction.CREATE, this.topologyListenerTriple.get(0).getRight());

        assertEquals(aVertexLabel, this.topologyListenerTriple.get(1).getLeft());
        Map<String, PropertyColumn> props = ((VertexLabel) this.topologyListenerTriple.get(1).getLeft()).getProperties();
        assertTrue(props.containsKey("name"));
        assertTrue(props.containsKey("surname"));

        assertNull(this.topologyListenerTriple.get(1).getMiddle());
        assertEquals(TopologyChangeAction.CREATE, this.topologyListenerTriple.get(1).getRight());

        assertEquals(edgeLabel, this.topologyListenerTriple.get(2).getLeft());
        String s = this.topologyListenerTriple.get(2).getLeft().toString();
        assertTrue(s.contains(edgeLabel.getSchema().getName()));
        props = ((EdgeLabel) this.topologyListenerTriple.get(2).getLeft()).getProperties();
        assertTrue(props.containsKey("special"));
        assertNull(this.topologyListenerTriple.get(2).getMiddle());
        assertEquals(TopologyChangeAction.CREATE, this.topologyListenerTriple.get(2).getRight());

        assertEquals(vertexPropertyColumn, this.topologyListenerTriple.get(3).getLeft());
        assertNull(this.topologyListenerTriple.get(3).getMiddle());
        assertEquals(TopologyChangeAction.CREATE, this.topologyListenerTriple.get(3).getRight());

        assertEquals(edgePropertyColumn, this.topologyListenerTriple.get(4).getLeft());
        assertNull(this.topologyListenerTriple.get(4).getMiddle());
        assertEquals(TopologyChangeAction.CREATE, this.topologyListenerTriple.get(4).getRight());

        assertEquals(bVertexLabel, this.topologyListenerTriple.get(5).getLeft());
        assertNull(this.topologyListenerTriple.get(5).getMiddle());
        assertEquals(TopologyChangeAction.CREATE, this.topologyListenerTriple.get(5).getRight());

        assertEquals(edgeLabel, this.topologyListenerTriple.get(6).getLeft());
        assertEquals(bVertexLabel, this.topologyListenerTriple.get(6).getMiddle());
        assertEquals(TopologyChangeAction.ADD_IN_VERTEX_LABELTO_EDGE, this.topologyListenerTriple.get(6).getRight());

        assertEquals(index, this.topologyListenerTriple.get(7).getLeft());
        assertNull(this.topologyListenerTriple.get(7).getMiddle());
        assertEquals(TopologyChangeAction.CREATE, this.topologyListenerTriple.get(7).getRight());

        this.sqlgGraph.tx().commit();
    }

    public static class TopologyListenerTest implements TopologyListener {
        private List<Triple<TopologyInf, TopologyInf, TopologyChangeAction>> topologyListenerTriple = new ArrayList<>();

        public TopologyListenerTest(List<Triple<TopologyInf, TopologyInf, TopologyChangeAction>> topologyListenerTriple) {
            super();
            this.topologyListenerTriple = topologyListenerTriple;
        }

        public TopologyListenerTest() {

        }

        @Override
        public void change(TopologyInf topologyInf, TopologyInf oldValue, TopologyChangeAction action) {
            String s = topologyInf.toString();
            assertNotNull(s);
            assertTrue(s + "does not contain " + topologyInf.getName(), s.contains(topologyInf.getName()));
            topologyListenerTriple.add(
                    Triple.of(topologyInf, oldValue, action)
            );
        }

        public boolean receivedEvent(TopologyInf topologyInf, TopologyChangeAction action) {
            for (Triple<TopologyInf, TopologyInf, TopologyChangeAction> t : topologyListenerTriple) {
                if (t.getLeft().equals(topologyInf) && t.getRight().equals(action)) {
                    return true;
                }
            }
            return false;
        }

        public void reset() {
            topologyListenerTriple.clear();
        }
    }
}
