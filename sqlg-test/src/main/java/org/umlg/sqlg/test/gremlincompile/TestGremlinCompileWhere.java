package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Created by pieter on 2015/08/03.
 */
public class TestGremlinCompileWhere extends BaseTest {

    @Test
    public void testEquals() {
        this.sqlgGraph.addVertex(T.label,  "Person", "name", "johnny");
        this.sqlgGraph.addVertex(T.label,  "Person", "name", "pietie");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "koosie");
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("name", P.eq("johnny")).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("name", P.neq("johnny")).toList();
        Assert.assertEquals(2, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("name", P.lt("z")).toList();
        Assert.assertEquals(3, vertices.size());
    }
}
