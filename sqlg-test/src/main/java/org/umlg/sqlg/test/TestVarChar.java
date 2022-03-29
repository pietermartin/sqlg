package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.VertexLabel;

import java.util.HashMap;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/07/21
 */
public class TestVarChar extends BaseTest {

    @Test
    public void testVarChar() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                this.sqlgGraph.getSqlDialect().getPublicSchema(),
                "A",
                new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(10)));
                }});
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                this.sqlgGraph.getSqlDialect().getPublicSchema(),
                "B",
                new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(10)));
                }});
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                new HashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.varChar(10)));
                }});
        this.sqlgGraph.tx().commit();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "halo");
        this.sqlgGraph.addVertex(T.label, "A", "name", "halo");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "halo");
        a.addEdge("ab", b, "name", "halo");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("ab").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").outE("ab").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").out("ab").count().next(), 0);
        try {
            this.sqlgGraph.addVertex(T.label, "A", "name", "123456789101");
            Assert.fail("should not be able to add a \"name\" with more than 10 characters!");
        } catch (Exception ignore) {
            this.sqlgGraph.tx().rollback();
        }
        try {
            a.addEdge("ab", b, "name", "123456789101");
            Assert.fail("should not be able to add a \"name\" with more than 10 characters!");
        } catch (Exception ignore) {
        }
    }
}
