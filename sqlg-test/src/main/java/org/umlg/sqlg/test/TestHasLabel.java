package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Date: 2014/07/29
 * Time: 2:21 PM
 */
public class TestHasLabel extends BaseTest {

    @Test
    public void testHasLabel() {
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.tx().commit();

        Assert.assertEquals(8, this.sqlG.V().has(T.label, "Person").count().next(), 0);
    }

    @Test
    public void testNonExistingLabel() {
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.tx().commit();
        Assert.assertEquals(2, this.sqlG.V().has(T.label, "Person").count().next(), 0);
        Assert.assertEquals(0, this.sqlG.V().has(T.label, "Animal").count().next(), 0);
    }

    @Test
    public void testInLabels() {
        Vertex a = this.sqlG.addVertex(T.label, "Person", "name", "a");
        Vertex b = this.sqlG.addVertex(T.label, "Person", "name", "b");
        Vertex c = this.sqlG.addVertex(T.label, "Person", "name", "c");
        Vertex d = this.sqlG.addVertex(T.label, "Person", "name", "d");
        a.addEdge("knows", b);
        a.addEdge("created", b);
        a.addEdge("knows", c);
        a.addEdge("created", d);
        this.sqlG.tx().commit();
        Assert.assertEquals(4, this.sqlG.V().has(T.label, "Person").count().next(), 0);
        Assert.assertEquals(2, this.sqlG.E().has(T.label, Contains.in, Arrays.asList("knows")).count().next(), 0);
        Assert.assertEquals(2, this.sqlG.E().has(T.label, Contains.in, Arrays.asList("created")).count().next(), 0);
        Assert.assertEquals(4, this.sqlG.E().has(T.label, Contains.in, Arrays.asList("knows", "created")).count().next(), 0);
    }

}
