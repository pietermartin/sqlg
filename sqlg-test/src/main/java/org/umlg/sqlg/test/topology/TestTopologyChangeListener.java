package org.umlg.sqlg.test.topology;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2017/01/22
 * Time: 6:58 PM
 */
public class TestTopologyChangeListener extends BaseTest {

    private List<Triple<TopologyInf, String, TopologyChangeAction>> topologyListenerTriple = new ArrayList<>();

    @Before
    public void before() throws Exception {
        super.before();
        this.topologyListenerTriple.clear();
    }

    @Test
    public void testAddSchemaAndVertexAndEdge() {
        TopologyListenerTest topologyListenerTest = new TopologyListenerTest();
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "asda");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "asdasd");
        Edge e1 = a1.addEdge("aa", a2);
        a1.property("surname", "asdasd");
        e1.property("special", "");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "A.B", "name", "asdasd");
        Edge e2 = a1.addEdge("aa", b1);

        Assert.assertEquals(7, this.topologyListenerTriple.size());

        Schema schema = this.sqlgGraph.getTopology().getSchema("A").get();
        VertexLabel aVertexLabel = schema.getVertexLabel("A").get();
        EdgeLabel edgeLabel = aVertexLabel.getOutEdgeLabel("aa").get();
        PropertyColumn vertexPropertyColumn = aVertexLabel.getProperty("surname").get();
        PropertyColumn edgePropertyColumn = edgeLabel.getProperty("special").get();
        VertexLabel bVertexLabel = schema.getVertexLabel("B").get();

        Assert.assertEquals(schema, this.topologyListenerTriple.get(0).getLeft());
        Assert.assertEquals("", this.topologyListenerTriple.get(0).getMiddle());
        Assert.assertEquals(TopologyChangeAction.CREATE, this.topologyListenerTriple.get(0).getRight());

        Assert.assertEquals(aVertexLabel, this.topologyListenerTriple.get(1).getLeft());
        Assert.assertEquals("", this.topologyListenerTriple.get(1).getMiddle());
        Assert.assertEquals(TopologyChangeAction.CREATE, this.topologyListenerTriple.get(1).getRight());

        Assert.assertEquals(edgeLabel, this.topologyListenerTriple.get(2).getLeft());
        Assert.assertEquals("", this.topologyListenerTriple.get(2).getMiddle());
        Assert.assertEquals(TopologyChangeAction.CREATE, this.topologyListenerTriple.get(2).getRight());

        Assert.assertEquals(vertexPropertyColumn, this.topologyListenerTriple.get(3).getLeft());
        Assert.assertEquals("", this.topologyListenerTriple.get(3).getMiddle());
        Assert.assertEquals(TopologyChangeAction.CREATE, this.topologyListenerTriple.get(3).getRight());

        Assert.assertEquals(edgePropertyColumn, this.topologyListenerTriple.get(4).getLeft());
        Assert.assertEquals("", this.topologyListenerTriple.get(4).getMiddle());
        Assert.assertEquals(TopologyChangeAction.CREATE, this.topologyListenerTriple.get(4).getRight());

        Assert.assertEquals(bVertexLabel, this.topologyListenerTriple.get(5).getLeft());
        Assert.assertEquals("", this.topologyListenerTriple.get(5).getMiddle());
        Assert.assertEquals(TopologyChangeAction.CREATE, this.topologyListenerTriple.get(5).getRight());

        Assert.assertEquals(edgeLabel, this.topologyListenerTriple.get(6).getLeft());
        Assert.assertEquals("A.B", this.topologyListenerTriple.get(6).getMiddle());
        Assert.assertEquals(TopologyChangeAction.ADD_IN_VERTEX_LABELTO_EDGE, this.topologyListenerTriple.get(6).getRight());

        this.sqlgGraph.tx().commit();
    }

    public class TopologyListenerTest implements TopologyListener {

        @Override
        public void change(TopologyInf topologyInf, String oldValue, TopologyChangeAction action) {
            TestTopologyChangeListener.this.topologyListenerTriple.add(
                    Triple.of(topologyInf, oldValue, action)
            );
        }
    }
}
